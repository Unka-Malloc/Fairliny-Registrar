//! Windows credentials use a protected DACL containing only the current user.
//! Replacing the entire DACL also removes unrelated explicit grants.

#[derive(Clone, Copy, Default)]
struct AcePolicy {
    access_allowed: bool,
    current_user: bool,
    full_control: bool,
    private_flags: bool,
}

fn owner_only(owner_is_user: bool, protected: bool, entries: &[AcePolicy]) -> bool {
    owner_is_user
        && protected
        && matches!(entries, [entry] if entry.access_allowed
            && entry.current_user && entry.full_control && entry.private_flags)
}

#[cfg(windows)]
pub(crate) use os::{harden, validate};

#[cfg(windows)]
mod os {
    use super::{AcePolicy, owner_only};
    use anyhow::{Result, anyhow};
    use std::{
        ffi::c_void,
        fs,
        mem::{offset_of, size_of},
        os::windows::{ffi::OsStrExt, fs::MetadataExt},
        path::Path,
        ptr::{addr_of, null_mut},
    };
    use windows_sys::Win32::{
        Foundation::{CloseHandle, ERROR_INSUFFICIENT_BUFFER, GetLastError, HANDLE, LocalFree},
        Security::{
            ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_REVISION, AddAccessAllowedAceEx,
            Authorization::{GetNamedSecurityInfoW, SE_FILE_OBJECT, SetNamedSecurityInfoW},
            CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetLengthSid,
            GetSecurityDescriptorControl, GetTokenInformation, InitializeAcl, IsValidAcl,
            IsValidSid, OBJECT_INHERIT_ACE, OWNER_SECURITY_INFORMATION,
            PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
            TOKEN_QUERY, TOKEN_USER, TokenUser,
        },
        Storage::FileSystem::{FILE_ALL_ACCESS, FILE_ATTRIBUTE_REPARSE_POINT},
        System::{
            SystemServices::ACCESS_ALLOWED_ACE_TYPE,
            Threading::{GetCurrentProcess, OpenProcessToken},
        },
    };

    fn unsafe_state() -> anyhow::Error {
        anyhow!("mcp_state_unsafe")
    }

    struct Token(HANDLE);
    impl Drop for Token {
        fn drop(&mut self) {
            // SAFETY: this handle was returned by OpenProcessToken and is owned here.
            unsafe { CloseHandle(self.0) };
        }
    }

    // Word storage gives TOKEN_USER and its SID the alignment required by Win32.
    struct CurrentUser(Vec<usize>);
    impl CurrentUser {
        fn query() -> Result<Self> {
            // SAFETY: query the required byte count first, then supply aligned
            // storage of that size; the token handle stays live until both calls end.
            unsafe {
                let mut token = null_mut();
                if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
                    return Err(unsafe_state());
                }
                let token = Token(token);
                let mut needed = 0;
                if GetTokenInformation(token.0, TokenUser, null_mut(), 0, &mut needed) != 0
                    || GetLastError() != ERROR_INSUFFICIENT_BUFFER
                    || needed < size_of::<TOKEN_USER>() as u32
                {
                    return Err(unsafe_state());
                }
                let mut user = Self(vec![0; (needed as usize).div_ceil(size_of::<usize>())]);
                if GetTokenInformation(
                    token.0,
                    TokenUser,
                    user.0.as_mut_ptr().cast(),
                    needed,
                    &mut needed,
                ) == 0
                    || IsValidSid(user.sid()) == 0
                {
                    return Err(unsafe_state());
                }
                Ok(user)
            }
        }

        fn sid(&self) -> PSID {
            // SAFETY: GetTokenInformation initialized this aligned TOKEN_USER buffer;
            // its embedded SID remains alive for the lifetime of this allocation.
            unsafe { (*self.0.as_ptr().cast::<TOKEN_USER>()).User.Sid }
        }
    }

    struct Descriptor {
        storage: PSECURITY_DESCRIPTOR,
        owner: PSID,
        dacl: *mut ACL,
    }
    impl Drop for Descriptor {
        fn drop(&mut self) {
            // SAFETY: GetNamedSecurityInfoW allocates this buffer with LocalAlloc.
            unsafe { LocalFree(self.storage) };
        }
    }
    impl Descriptor {
        fn query(path: &[u16]) -> Result<Self> {
            let mut descriptor = Self {
                storage: null_mut(),
                owner: null_mut(),
                dacl: null_mut(),
            };
            // SAFETY: the path is NUL-terminated and every output pointer is valid.
            let status = unsafe {
                GetNamedSecurityInfoW(
                    path.as_ptr(),
                    SE_FILE_OBJECT,
                    OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                    &mut descriptor.owner,
                    null_mut(),
                    &mut descriptor.dacl,
                    null_mut(),
                    &mut descriptor.storage,
                )
            };
            if status != 0 || descriptor.storage.is_null() {
                return Err(unsafe_state());
            }
            Ok(descriptor)
        }

        fn owned_by(&self, user: &CurrentUser) -> bool {
            // SAFETY: non-null owner points into the live OS descriptor allocation.
            !self.owner.is_null()
                && unsafe { IsValidSid(self.owner) != 0 && EqualSid(self.owner, user.sid()) != 0 }
        }

        fn validate(&self, user: &CurrentUser, directory: bool) -> Result<()> {
            // SAFETY: the descriptor comes from Win32 and lives through this call.
            // IsValidAcl and GetAce validate the ACL before entry fields are read.
            unsafe {
                let mut control = 0;
                let mut revision = 0;
                if self.dacl.is_null()
                    || IsValidAcl(self.dacl) == 0
                    || GetSecurityDescriptorControl(self.storage, &mut control, &mut revision) == 0
                    || (*self.dacl).AceCount != 1
                {
                    return Err(unsafe_state());
                }
                let mut raw: *mut c_void = null_mut();
                if GetAce(self.dacl, 0, &mut raw) == 0 || raw.is_null() {
                    return Err(unsafe_state());
                }
                let header = &*raw.cast::<ACE_HEADER>();
                let sid_offset = offset_of!(ACCESS_ALLOWED_ACE, SidStart);
                if header.AceType as u32 != ACCESS_ALLOWED_ACE_TYPE
                    || (header.AceSize as usize) < sid_offset + 8
                {
                    return Err(unsafe_state());
                }
                let entry = &*raw.cast::<ACCESS_ALLOWED_ACE>();
                let sid = addr_of!(entry.SidStart).cast_mut().cast::<c_void>();
                // A SID has an 8-byte header followed by its u32 subauthorities.
                // Bound the variable-length SID before asking Win32 to inspect it.
                let sid_bytes = 8 + 4 * *sid.cast::<u8>().add(1) as usize;
                if sid_offset + sid_bytes > header.AceSize as usize || IsValidSid(sid) == 0 {
                    return Err(unsafe_state());
                }
                let entry = AcePolicy {
                    access_allowed: true,
                    current_user: EqualSid(sid, user.sid()) != 0,
                    full_control: entry.Mask == FILE_ALL_ACCESS,
                    private_flags: header.AceFlags as u32 == inheritance(directory),
                };
                if !owner_only(
                    self.owned_by(user),
                    control & SE_DACL_PROTECTED != 0,
                    &[entry],
                ) {
                    return Err(unsafe_state());
                }
                Ok(())
            }
        }
    }

    fn inheritance(directory: bool) -> u32 {
        if directory {
            OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE
        } else {
            0
        }
    }

    fn checked_path(path: &Path) -> Result<(Vec<u16>, bool)> {
        let metadata = fs::symlink_metadata(path).map_err(|_| unsafe_state())?;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            || !(metadata.is_file() || metadata.is_dir())
        {
            return Err(unsafe_state());
        }
        let mut wide: Vec<_> = path.as_os_str().encode_wide().collect();
        if wide.contains(&0) {
            return Err(unsafe_state());
        }
        wide.push(0);
        Ok((wide, metadata.is_dir()))
    }

    pub(crate) fn validate(path: &Path) -> Result<()> {
        let (path, directory) = checked_path(path)?;
        let user = CurrentUser::query()?;
        Descriptor::query(&path)?.validate(&user, directory)
    }

    /// Call after creating an empty file and before writing any secret bytes.
    pub(crate) fn harden(path: &Path) -> Result<()> {
        let (path, directory) = checked_path(path)?;
        let user = CurrentUser::query()?;
        if !Descriptor::query(&path)?.owned_by(&user) {
            return Err(unsafe_state());
        }
        // SAFETY: the SID was validated when the token buffer was loaded. The
        // aligned ACL allocation includes its header, one ACE and the complete SID.
        unsafe {
            let bytes = size_of::<ACL>()
                + offset_of!(ACCESS_ALLOWED_ACE, SidStart)
                + GetLengthSid(user.sid()) as usize;
            let mut storage = vec![0u32; bytes.div_ceil(size_of::<u32>())];
            let acl = storage.as_mut_ptr().cast::<ACL>();
            if InitializeAcl(acl, bytes as u32, ACL_REVISION) == 0
                || AddAccessAllowedAceEx(
                    acl,
                    ACL_REVISION,
                    inheritance(directory),
                    FILE_ALL_ACCESS,
                    user.sid(),
                ) == 0
                || SetNamedSecurityInfoW(
                    path.as_ptr(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                    null_mut(),
                    null_mut(),
                    acl,
                    null_mut(),
                ) != 0
            {
                return Err(unsafe_state());
            }
        }
        Descriptor::query(&path)?.validate(&user, directory)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use windows_sys::Win32::Security::{
            CreateWellKnownSid, SECURITY_MAX_SID_SIZE, WinWorldSid,
        };

        fn add_explicit_everyone_grant(path: &[u16]) {
            let user = CurrentUser::query().unwrap();
            let mut world = [0u32; (SECURITY_MAX_SID_SIZE as usize).div_ceil(4)];
            let mut world_size = SECURITY_MAX_SID_SIZE;
            let world_sid = world.as_mut_ptr().cast();
            unsafe {
                assert_ne!(
                    CreateWellKnownSid(WinWorldSid, null_mut(), world_sid, &mut world_size),
                    0
                );
                let bytes = size_of::<ACL>()
                    + 2 * offset_of!(ACCESS_ALLOWED_ACE, SidStart)
                    + GetLengthSid(user.sid()) as usize
                    + world_size as usize;
                let mut storage = vec![0u32; bytes.div_ceil(4)];
                let acl = storage.as_mut_ptr().cast();
                assert_ne!(InitializeAcl(acl, bytes as u32, ACL_REVISION), 0);
                for sid in [user.sid(), world_sid] {
                    assert_ne!(
                        AddAccessAllowedAceEx(acl, ACL_REVISION, 0, FILE_ALL_ACCESS, sid),
                        0
                    );
                }
                assert_eq!(
                    SetNamedSecurityInfoW(
                        path.as_ptr(),
                        SE_FILE_OBJECT,
                        DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                        null_mut(),
                        null_mut(),
                        acl,
                        null_mut(),
                    ),
                    0
                );
            }
        }

        #[test]
        fn hardening_replaces_unsafe_dacls_and_protects_new_children() {
            let directory = std::env::temp_dir().join(format!("mcp-acl-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&directory).unwrap();
            harden(&directory).unwrap();
            validate(&directory).unwrap();
            let file = directory.join("synthetic-token");
            fs::File::create(&file).unwrap();
            // Inherited owner access is safe during creation, but an existing
            // published credential requires its own protected, explicit DACL.
            assert!(validate(&file).is_err());
            harden(&file).unwrap();
            validate(&file).unwrap();
            let (path, _) = checked_path(&file).unwrap();
            add_explicit_everyone_grant(&path);
            assert!(validate(&file).is_err());
            harden(&file).unwrap();
            validate(&file).unwrap();
            unsafe {
                assert_eq!(
                    SetNamedSecurityInfoW(
                        path.as_ptr(),
                        SE_FILE_OBJECT,
                        DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                        null_mut(),
                        null_mut(),
                        null_mut(),
                        null_mut(),
                    ),
                    0
                );
            }
            assert!(validate(&file).is_err());
            harden(&file).unwrap();
            validate(&file).unwrap();
            fs::remove_dir_all(directory).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user_entry() -> AcePolicy {
        AcePolicy {
            access_allowed: true,
            current_user: true,
            full_control: true,
            private_flags: true,
        }
    }

    #[test]
    fn credential_policy_requires_owner_and_protected_single_user_acl() {
        assert!(owner_only(true, true, &[user_entry()]));
        assert!(!owner_only(false, true, &[user_entry()]));
        assert!(!owner_only(true, false, &[user_entry()]));
        assert!(!owner_only(true, true, &[]));
    }

    #[test]
    fn explicit_foreign_grants_are_never_preserved_or_accepted() {
        let foreign = AcePolicy {
            current_user: false,
            ..user_entry()
        };
        assert!(!owner_only(true, true, &[foreign]));
        assert!(!owner_only(true, true, &[user_entry(), foreign]));
        assert!(!owner_only(true, true, &[user_entry(), user_entry()]));
    }

    #[test]
    fn nonstandard_or_ineffective_user_entries_fail_closed() {
        for entry in [
            AcePolicy {
                access_allowed: false,
                ..user_entry()
            },
            AcePolicy {
                full_control: false,
                ..user_entry()
            },
            AcePolicy {
                private_flags: false,
                ..user_entry()
            },
        ] {
            assert!(!owner_only(true, true, &[entry]));
        }
    }
}
