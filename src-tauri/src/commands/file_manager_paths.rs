use percent_encoding::percent_decode_str;

pub(super) const UNSUPPORTED_PREFIX: &str = "Unsupported:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RemotePath(String);

impl RemotePath {
    pub(super) fn parse(input: &str) -> Result<Self, String> {
        let decoded = percent_decode_str(input)
            .decode_utf8()
            .map_err(|_| "Remote path contains invalid percent-encoded UTF-8".to_string())?;
        if decoded.is_empty() {
            return Err("Remote path is required".to_string());
        }
        if decoded.trim() != decoded {
            return Err(
                "Remote path cannot begin or end with whitespace because the storage runtime would normalize it"
                    .to_string(),
            );
        }
        if decoded.starts_with('/') {
            return Err("Remote path must be relative to the configured root".to_string());
        }
        if decoded.contains('\0') || decoded.contains('\\') {
            return Err("Remote path contains an invalid character".to_string());
        }

        let without_trailing_slash = decoded.strip_suffix('/').unwrap_or(&decoded);
        if without_trailing_slash.is_empty() {
            return Err("The configured root cannot be changed or deleted".to_string());
        }

        let mut segments = Vec::new();
        for segment in without_trailing_slash.split('/') {
            if segment.is_empty() {
                return Err("Remote path cannot contain empty path segments".to_string());
            }
            if matches!(segment, "." | "..") {
                return Err("Remote path cannot contain '.' or '..' path segments".to_string());
            }
            segments.push(segment);
        }
        Ok(Self(segments.join("/")))
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

pub(super) fn join_configured_root(root: &str, path: &RemotePath, directory: bool) -> String {
    let root = root.trim_matches('/');
    let mut joined = if root.is_empty() { path.as_str().to_string() } else { format!("{root}/{}", path.as_str()) };
    if directory {
        joined.push('/');
    }
    joined
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DirectoryStorageModel {
    Hierarchical,
    ObjectStore,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DirectoryDeleteEvidence {
    pub(super) has_children: bool,
    pub(super) marker_size: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DirectoryDeletePlan {
    DeleteExactDirectory,
    DeleteExactMarker,
    NoOpVirtualPrefix,
}

pub(super) fn plan_directory_delete(
    model: DirectoryStorageModel,
    evidence: DirectoryDeleteEvidence,
) -> Result<DirectoryDeletePlan, String> {
    if evidence.has_children {
        return Err("Directory is not empty; recursive delete is unsupported".to_string());
    }
    match model {
        DirectoryStorageModel::Hierarchical => Ok(DirectoryDeletePlan::DeleteExactDirectory),
        DirectoryStorageModel::ObjectStore => match evidence.marker_size {
            None => Ok(DirectoryDeletePlan::NoOpVirtualPrefix),
            Some(0) => Ok(DirectoryDeletePlan::DeleteExactMarker),
            Some(_) => Err("Directory marker is not empty and cannot be deleted safely".to_string()),
        },
    }
}

pub(super) fn reject_recursive_delete(recursive: bool) -> Result<(), String> {
    if recursive {
        Err(format!("{UNSUPPORTED_PREFIX} recursive directory delete is not available in v1"))
    } else {
        Ok(())
    }
}

#[allow(dead_code)]
pub(super) fn reject_directory_copy_or_rename(operation: &str) -> Result<(), String> {
    match operation {
        "copy" | "rename" => Err(format!("{UNSUPPORTED_PREFIX} directory {operation} is not available in v1")),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_paths_are_root_relative_after_percent_decoding() {
        for unsafe_path in [
            "/absolute",
            ".",
            "..",
            "safe/../escape",
            "safe/./escape",
            "safe\\escape",
            "safe\0escape",
            "%2Fabsolute",
            "%2e",
            "%2E%2E",
            "safe%2f..%2fescape",
            "safe%5cescape",
            "safe%00escape",
            "file ",
            "%20file",
        ] {
            assert!(RemotePath::parse(unsafe_path).is_err(), "{unsafe_path:?} must be rejected");
        }
        assert_eq!(RemotePath::parse("safe%20name/report.txt").unwrap().as_str(), "safe name/report.txt");
        assert_eq!(RemotePath::parse("folder/").unwrap().as_str(), "folder");
    }

    #[test]
    fn accepted_path_property_never_emits_unsafe_segments() {
        let alphabet = ["a", "b", "/", ".", "%2e", "%2f", "%5c", "%00"];
        for mut value in 0_u64..65_536 {
            let mut candidate = String::new();
            for _ in 0..6 {
                candidate.push_str(alphabet[(value as usize) % alphabet.len()]);
                value /= alphabet.len() as u64;
            }
            if let Ok(path) = RemotePath::parse(&candidate) {
                assert!(!path.as_str().starts_with('/'));
                assert!(!path.as_str().contains('\\'));
                assert!(!path.as_str().contains('\0'));
                assert!(path.as_str().split('/').all(|segment| !matches!(segment, "" | "." | "..")));
            }
        }
    }

    #[test]
    fn configured_root_join_cannot_escape() {
        let path = RemotePath::parse("child/file.txt").unwrap();
        assert_eq!(join_configured_root("/tenant/root", &path, false), "tenant/root/child/file.txt");
        assert_eq!(join_configured_root("/", &path, true), "child/file.txt/");
    }

    #[test]
    fn object_store_delete_never_bulk_deletes_a_prefix() {
        assert_eq!(
            plan_directory_delete(
                DirectoryStorageModel::ObjectStore,
                DirectoryDeleteEvidence { has_children: false, marker_size: None }
            )
            .unwrap(),
            DirectoryDeletePlan::NoOpVirtualPrefix
        );
        assert_eq!(
            plan_directory_delete(
                DirectoryStorageModel::ObjectStore,
                DirectoryDeleteEvidence { has_children: false, marker_size: Some(0) }
            )
            .unwrap(),
            DirectoryDeletePlan::DeleteExactMarker
        );
        assert!(plan_directory_delete(
            DirectoryStorageModel::ObjectStore,
            DirectoryDeleteEvidence { has_children: true, marker_size: Some(0) }
        )
        .is_err());
        assert!(plan_directory_delete(
            DirectoryStorageModel::ObjectStore,
            DirectoryDeleteEvidence { has_children: false, marker_size: Some(1) }
        )
        .is_err());
    }

    #[test]
    fn unsupported_directory_operations_have_stable_classification() {
        assert!(reject_recursive_delete(true).unwrap_err().starts_with(UNSUPPORTED_PREFIX));
        assert!(reject_directory_copy_or_rename("copy").unwrap_err().starts_with(UNSUPPORTED_PREFIX));
        assert!(reject_directory_copy_or_rename("rename").unwrap_err().starts_with(UNSUPPORTED_PREFIX));
    }
}
