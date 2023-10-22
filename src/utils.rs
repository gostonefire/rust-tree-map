use std::fs::File;
use crate::TreeFileError;
use crate::TreeFileError::{FileIOError, LogicError};

pub fn create_file(path: &str) -> Result<File, TreeFileError> {
    File::options()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(path)
        .map_err(|e| FileIOError {
            msg: format!("Error while creating file {}: {}", path, e)
        })
}

pub fn open_file(path: &str) -> Result<File, TreeFileError> {
    File::options()
        .write(true)
        .read(true)
        .open(path)
        .map_err(|e| FileIOError {
            msg: format!("Error while opening file {}: {}", path, e)
        })
}

pub fn add_and_subtract(mut value: u64, add: i64) -> Result<u64, TreeFileError> {
    if add < 0 {
        let a = add.abs() as u64;
        if a > value {
            return Err(LogicError {
                msg: String::from("Would subtract below zero on unsigned value (u64)")
            });
        }
        value -= a;
    } else {
        value += add as u64;
    }

    Ok(value)
}