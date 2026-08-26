using System.Buffers.Binary;

namespace Nexum.Sdk;

/// <summary>Binary writer for the Nexum wire protocol.</summary>
public sealed class Writer
{
    private readonly List<byte> _buf = new(256);

    public void U8(byte v) => _buf.Add(v);
    public void U16(ushort v) => _buf.AddRange(BitConverter.GetBytes(v));
    public void U32(uint v) => _buf.AddRange(BitConverter.GetBytes(v));
    public void U64(ulong v) => _buf.AddRange(BitConverter.GetBytes(v));
    public void I32(int v) => _buf.AddRange(BitConverter.GetBytes(v));
    public void I64(long v) => _buf.AddRange(BitConverter.GetBytes(v));

    public void Str(string s)
    {
        var bytes = Encoding.UTF8.GetBytes(s);
        U32((uint)bytes.Length);
        _buf.AddRange(bytes);
    }

    public byte[] Data() => _buf.ToArray();
}
