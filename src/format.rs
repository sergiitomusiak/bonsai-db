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
