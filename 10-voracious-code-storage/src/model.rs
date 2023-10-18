use std::collections::HashMap;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PathError {
    #[error("illegal file name")]
    IllegalFileName,
    #[error("illegal dir name")]
    IllegalDirectoryName,
}

pub struct FileSystem {
    root: Directory,
}

#[derive(Debug)]
pub struct File {
    pub revisions: Vec<Vec<u8>>,
}

#[derive(Debug)]
pub struct Directory {
    pub files: HashMap<String, File>,
    pub directories: HashMap<String, Directory>,
}

#[derive(Debug)]
pub struct FilePath {
    parts: Vec<String>,
}

#[derive(Debug)]
pub struct DirectoryPath {
    parts: Vec<String>,
}

impl Default for FileSystem {
    fn default() -> Self {
        FileSystem {
            root: Directory {
                files: HashMap::new(),
                directories: HashMap::new(),
            },
        }
    }
}

impl FileSystem {
    pub fn get_file(&self, path: &FilePath) -> Option<&File> {
        let dir_parts = &path.parts[..(path.parts.len() - 1)];
        let file_part = &path.parts[path.parts.len() - 1];

        let mut curr_dir = &self.root;
        for dir_part in dir_parts {
            match curr_dir.directories.get(dir_part) {
                Some(v) => curr_dir = v,
                None => return None,
            }
        }
        curr_dir.files.get(file_part)
    }

    pub fn put_file(&mut self, path: &FilePath, data: Vec<u8>) -> u32 {
        let dir_parts = &path.parts[..(path.parts.len() - 1)];
        let file_part = &path.parts[path.parts.len() - 1];

        let mut curr_dir = &mut self.root;
        for dir_part in dir_parts {
            curr_dir = match curr_dir.directories.entry(dir_part.to_string()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => entry.insert(Directory {
                    files: HashMap::new(),
                    directories: HashMap::new(),
                }),
            }
        }

        let file = match curr_dir.files.entry(file_part.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => entry.insert(File {
                revisions: Vec::new(),
            }),
        };
        if file
            .revisions
            .last()
            .map(|last| last != &data)
            .unwrap_or(true)
        {
            file.revisions.push(data);
        }

        file.revisions.len() as u32
    }

    pub fn get_directory(&self, path: &DirectoryPath) -> Option<&Directory> {
        if path.parts.is_empty() {
            return Some(&self.root);
        }
        let dir_parts = &path.parts[..];

        let mut curr_dir = &self.root;
        for dir_part in dir_parts {
            match curr_dir.directories.get(dir_part) {
                Some(v) => curr_dir = v,
                None => return None,
            }
        }
        Some(curr_dir)
    }
}

impl FilePath {
    pub fn from_str(file_path_str: &str) -> Result<FilePath, PathError> {
        if !file_path_str.starts_with('/') || !valid_path(file_path_str) {
            return Err(PathError::IllegalFileName);
        }
        let file_path_str = &file_path_str[1..];

        if file_path_str.ends_with('/') {
            return Err(PathError::IllegalFileName);
        }

        let parts = file_path_str.split('/').map(|v| v.to_string()).collect();

        Ok(FilePath { parts })
    }
}

impl DirectoryPath {
    pub fn from_str(directory_path_str: &str) -> Result<DirectoryPath, PathError> {
        if !directory_path_str.starts_with('/') || !valid_path(directory_path_str) {
            return Err(PathError::IllegalDirectoryName);
        }
        let directory_path_str = &directory_path_str[1..];
        if directory_path_str.is_empty() {
            return Ok(DirectoryPath { parts: Vec::new() });
        }
        let directory_path_str = directory_path_str
            .strip_suffix('/')
            .unwrap_or(directory_path_str);

        let parts = directory_path_str
            .split('/')
            .map(|v| v.to_string())
            .collect();

        Ok(DirectoryPath { parts })
    }
}

fn valid_path(path_str: &str) -> bool {
    !path_str.contains("//")
        && path_str
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '/' | '.'))
}
