// Copyright 2024 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(not(feature = "retry_buffer_tempfile"))]
pub(crate) type RetryBuffer = fixedbuffer::FixedBuffer;
#[cfg(feature = "retry_buffer_tempfile")]
pub(crate) type RetryBuffer = tempfile::TempFileBuffer;

#[cfg(not(feature = "retry_buffer_tempfile"))]
pub(crate) mod fixedbuffer {
    use bytes::{Bytes, BytesMut};

    /// A buffer with size limit. When the total amount of data written to the buffer is below the limit
    /// all the data will be held in the buffer. Otherwise, the buffer will report to be truncated.
    pub(crate) struct FixedBuffer {
        buffer: BytesMut,
        capacity: usize,
        truncated: bool,
    }

    impl FixedBuffer {
        pub fn new(capacity: usize) -> Self {
            FixedBuffer {
                buffer: BytesMut::new(),
                capacity,
                truncated: false,
            }
        }

        // TODO: maybe store a Vec of Bytes for zero-copy
        pub fn write_to_buffer(&mut self, data: &Bytes) {
            if !self.truncated && (self.buffer.len() + data.len() <= self.capacity) {
                self.buffer.extend_from_slice(data);
            } else {
                // TODO: clear data because the data held here is useless anyway?
                self.truncated = true;
            }
        }
        pub fn clear(&mut self) {
            self.truncated = false;
            self.buffer.clear();
        }
        pub fn is_empty(&self) -> bool {
            self.buffer.len() == 0
        }
        pub fn is_truncated(&self) -> bool {
            self.truncated
        }
        pub fn get_buffer(&self) -> Option<Bytes> {
            // TODO: return None if truncated?
            if !self.is_empty() {
                Some(self.buffer.clone().freeze())
            } else {
                None
            }
        }
    }
}

#[cfg(feature = "retry_buffer_tempfile")]
pub(crate) mod tempfile {
    use bytes::{BufMut as _, Bytes, BytesMut};
    use std::io::{Seek as _, SeekFrom, Write};
    use tempfile::SpooledTempFile;

    pub(crate) struct TempFileBuffer {
        spoolfile: std::sync::Mutex<SpooledTempFile>,
        truncated: std::sync::atomic::AtomicBool,
        capacity: usize,
        size: u64,
    }

    impl TempFileBuffer {
        pub fn new(capacity: usize) -> Self {
            TempFileBuffer {
                spoolfile: std::sync::Mutex::new(SpooledTempFile::new(capacity)),
                truncated: std::sync::atomic::AtomicBool::new(false),
                capacity,
                size: 0,
            }
        }

        pub fn write_to_buffer(&mut self, data: &Bytes) {
            if self.truncated.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }

            if let Err(err) = self.write_to_buffer_inner(data) {
                self.truncated
                    .store(true, std::sync::atomic::Ordering::Release);
                log::warn!("couldn't write to spoolfile: {err}")
            }
        }

        fn write_to_buffer_inner(&mut self, data: &Bytes) -> Result<(), String> {
            let data_size = u64::try_from(data.len()).map_err(|_| format!("buffer overflowed"))?;
            let new_size = self
                .size
                .checked_add(data_size)
                .ok_or_else(|| format!("buffer overflowed"))?;
            self.spoolfile
                .lock()
                .map_err(|e| format!("spoolfile locking error {:#?}", e))?
                .write_all(data)
                .map_err(|e| format!("{:#?}", e))?;
            self.size = new_size;
            Ok(())
        }

        pub fn clear(&mut self) {
            self.truncated
                .store(false, std::sync::atomic::Ordering::Release);
            self.spoolfile = std::sync::Mutex::new(SpooledTempFile::new(self.capacity));
            self.size = 0;
        }

        pub fn is_empty(&self) -> bool {
            self.size == 0
        }

        pub fn is_truncated(&self) -> bool {
            self.truncated.load(std::sync::atomic::Ordering::Acquire)
        }

        pub fn get_buffer(&self) -> Option<Bytes> {
            if self.is_empty() {
                return None;
            }

            if self.is_truncated() {
                log::warn!("trying to use truncated retry buffer");
                return None;
            }

            match self.get_buffer_inner() {
                Ok(bytes) => Some(bytes),
                Err(err) => {
                    self.truncated
                        .store(true, std::sync::atomic::Ordering::Release);
                    log::warn!("error trying to use retry buffer: {err}");
                    None
                }
            }
        }

        fn get_buffer_inner(&self) -> Result<Bytes, String> {
            let mut spoolfile = self
                .spoolfile
                .lock()
                .map_err(|e| format!("lock error: {:#?}", e))?;

            spoolfile
                .seek(SeekFrom::Start(0))
                .map_err(|err| format!("{:#?}", err))?;

            let size = usize::try_from(self.size).map_err(|_| "buffer overflowed".to_string())?;
            let mut bytes_writer = BytesMut::with_capacity(size).writer();
            std::io::copy(&mut *spoolfile, &mut bytes_writer).map_err(|e| format!("{:#?}", e))?;

            spoolfile
                .seek(SeekFrom::End(0))
                .map_err(|err| format!("{:#?}", err))?;

            Ok(bytes_writer.into_inner().freeze())
        }
    }
}
