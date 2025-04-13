use anyhow::Result;
use std::io::{Read, Write};

#[inline]
pub(crate) fn read_u64<R: Read>(reader: &mut R) -> Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

#[inline]
pub(crate) fn read_u32<R: Read>(reader: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

#[inline]
pub(crate) fn read_u16<R: Read>(reader: &mut R) -> Result<u16> {
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf)?;
    Ok(u16::from_be_bytes(buf))
}

#[inline]
pub(crate) fn read_vec_u64<R: Read>(reader: &mut R, len: usize) -> Result<Vec<u64>> {
    let mut buf = [0u8; 8];
    let mut res = Vec::new();
    for _ in 0..len {
        reader.read_exact(&mut buf)?;
        res.push(u64::from_be_bytes(buf));
    }
    Ok(res)
}

#[inline]
pub(crate) fn write_u64<W: Write>(w: &mut W, value: u64) -> Result<()> {
    let bytes = u64::to_be_bytes(value);
    w.write_all(&bytes)?;
    Ok(())
}

#[inline]
pub(crate) fn write_u32<W: Write>(w: &mut W, value: u32) -> Result<()> {
    let bytes = u32::to_be_bytes(value);
    w.write_all(&bytes)?;
    Ok(())
}

#[inline]
pub(crate) fn write_u16<W: Write>(w: &mut W, value: u16) -> Result<()> {
    let bytes = u16::to_be_bytes(value);
    w.write_all(&bytes)?;
    Ok(())
}

#[inline]
pub(crate) fn write_slice_u64<W: Write>(w: &mut W, values: &[u64]) -> Result<()> {
    for value in values {
        let bytes = u64::to_be_bytes(*value);
        w.write_all(&bytes)?;
    }
    Ok(())
}

// pub(crate) struct LimitedWriter<'a> {
//     left: usize,
//     file: &'a mut std::fs::File,
// }

// impl<'a> LimitedWriter<'a> {
//     pub(crate) fn new(file: &'a mut std::fs::File, offset: u64, size: usize) -> Result<Self> {
//         file.seek(std::io::SeekFrom::Start(offset))?;
//         Ok(Self {
//             file,
//             left: size,
//         })
//     }
// }

// impl<'a> Write for LimitedWriter<'a> {
//     fn write(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
//         if self.left == 0 {
//             return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "end of writer"));
//         }
//         if buf.len() > self.left {
//             buf = &buf[..self.left];
//         }
//         let written = self.file.write(buf)?;
//         self.left -= written;
//         Ok(written)
//     }

//     fn flush(&mut self) -> std::io::Result<()> {
//         self.file.flush()
//     }
// }

// // pub(crate) struct LimitedReader<'a> {
// //     left: usize,
// //     file: &'a mut std::fs::File,
// // }

// // impl<'a> LimitedReader<'a> {
// //     pub(crate) fn new(file: &'a mut std::fs::File, offset: u64, size: usize) -> Result<Self> {
// //         file.seek(std::io::SeekFrom::Start(offset))?;
// //         Ok(Self {
// //             file,
// //             left: size,
// //         })
// //     }
// // }

// // impl<'a> Read for LimitedReader<'a> {
// //     // Required method
// //     fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
// //         if self.left == 0 {
// //             return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected end of data"));
// //         }
// //         if buf.len() > self.left {
// //             buf = &mut buf[..self.left];
// //         }
// //         let read = self.file.read(buf)?;
// //         self.left -= read;
// //         Ok(read)
// //     }
// // }
