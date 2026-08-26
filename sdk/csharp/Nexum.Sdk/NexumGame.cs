using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;

namespace Nexum.Sdk;

/// <summary>Nexus game client — dead simple, extremely flexible.</summary>
/// <remarks>
/// Dead simple:
///   var game = new NexumGame("localhost", 9337);
///   await game.ConnectAsync();
///   await game.AuthAsync("token");
///   var players = game.Table("players", 32);
///   players.OnInsert(SpawnPlayer);
///   await game.CallAsync("move_player", new { dx = 1, dy = 0 });
///
/// Extremely flexible:
///   game.OnAny((kind, data) => { ... });
/// </remarks>
public sealed class NexumGame : IDisposable
{
    private TcpClient? _tcp;
    private NetworkStream? _stream;
    private CancellationTokenSource? _cts;
    private Task? _readLoop;

    private long _nextReqId = 1;
    private readonly ConcurrentDictionary<long, TaskCompletionSource<object?>> _pendingCalls = new();
    private readonly ConcurrentDictionary<string, TableView> _views = new();

    public ConnectionState State { get; private set; } = ConnectionState.Disconnected;

    // ─── connection ─────────────────────────────────────────────────

    public NexumGame(string host, int port) { Host = host; Port = port; }
    public string Host { get; }
    public int Port { get; }

    public async Task ConnectAsync(CancellationToken ct = default)
    {
        _tcp = new TcpClient();
        await _tcp.ConnectAsync(Host, Port, ct);
        _stream = _tcp.GetStream();
        State = ConnectionState.Connected;
        _cts = new CancellationTokenSource();
        _readLoop = Task.Run(() => ReadLoopAsync(_cts.Token));
    }

    public async Task DisconnectAsync()
    {
        _cts?.Cancel();
        _stream?.Close();
        _tcp?.Close();
        State = ConnectionState.Disconnected;
        await Task.CompletedTask;
    }

    // ─── auth ────────────────────────────────────────────────────────

    public async Task AuthAsync(string token, CancellationToken ct = default)
    {
        var w = new Writer();
        w.U16(1); // Authenticate
        w.Str(token);
        Send(w.Data());
        State = ConnectionState.Authenticated;
        await Task.CompletedTask;
    }

    public async Task AttachAsync(ulong worldId, CancellationToken ct = default)
    {
        var w = new Writer();
        w.U16(2); // AttachWorld
        w.U64(worldId);
        Send(w.Data());
        await Task.CompletedTask;
    }

    // ── tables (dead simple) ─────────────────────────────────────────

    /// <summary>Subscribe to a table with reactive updates.</summary>
    public TableView Table(string name, int limit = 32)
    {
        if (_views.TryGetValue(name, out var existing)) return existing;
        var view = new TableView(name, limit);
        _views[name] = view;

        var w = new Writer();
        w.U16(5); // Subscribe
        w.U64((ulong)Environment.TickCount64); // request id
        w.Str(name);
        w.U64(0); // no predicates
        w.U8(0);  // no order
        w.U8(1);  // has limit
        w.U64((ulong)limit);
        Send(w.Data());

        return view;
    }

    // ── reducers (dead simple) ───────────────────────────────────────

    public async Task CallAsync(string name, object? args = null, CancellationToken ct = default)
    {
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using reg = ct.Register(() => tcs.TrySetCanceled(ct));

        var reqId = Interlocked.Increment(ref _nextReqId) - 1;
        _pendingCalls[reqId] = tcs;

        var w = new Writer(256);
        w.U16(8); // CallReducer
        w.U64((ulong)reqId);
        w.Str(name);
        WriteArgs(w, args);
        Send(w.Data());

        await tcs.Task;
    }

    public void SendFireAndForget(string name, object? args = null)
    {
        var reqId = Interlocked.Increment(ref _nextReqId) - 1;
        _pendingCalls[reqId] = TaskCompletionSource<object?>.Create();
        var w = new Writer(256);
        w.U16(8);
        w.U64((ulong)reqId);
        w.Str(name);
        WriteArgs(w, args);
        Send(w.Data());
    }

    // ── input stream (lowest latency) ────────────────────────────────

    public void SendInput(ulong tick, params (ulong source, string kind, object? payload)[] commands)
    {
        var w = new Writer(256);
        w.U16(4); // InputFrame
        w.U64(tick);
        w.U64((ulong)commands.Length);
        foreach (var (source, kind, payload) in commands)
        {
            w.U64(source);
            w.Str(kind);
            if (payload is not null) { w.U8(1); WriteValue(w, payload); }
            else w.U8(0);
        }
        Send(w.Data());
    }

    // ── flexible layer ───────────────────────────────────────────────

    public event Action<int, byte[]>? OnAnyMessage;

    // ── internal ─────────────────────────────────────────────────────

    private void WriteArgs(Writer w, object? args)
    {
        if (args is null) { w.U64(0); return; }
        var props = args.GetType().GetProperties();
        w.U64((ulong)props.Length);
        foreach (var p in props.OrderBy(p => p.Name))
        {
            w.Str(p.Name);
            WriteValue(w, p.GetValue(args));
        }
    }

    private static void WriteValue(Writer w, object? v)
    {
        switch (v)
        {
            case bool b: w.U8(0); w.U8(b ? (byte)1 : (byte)0); break;
            case int i: w.U8(3); w.I32(i); break;
            case long l: w.U8(4); w.I64(l); break;
            case ulong ul: w.U8(8); w.U64(ul); break;
            case string s: w.U8(11); w.Str(s); break;
            default: w.U8(4); w.I64(0); break;
        }
    }

    private void Send(byte[] data)
    {
        if (_stream is null || !_stream.CanWrite)
            throw new InvalidOperationException("not connected");

        // Frame: [length u32][payload][crc32 u32]
        var frame = new byte[4 + data.Length + 4];
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(0), (uint)data.Length);
        Array.Copy(data, 0, frame, 4, data.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(4 + data.Length), Crc32(data));
        _stream.Write(frame);
    }

    private async Task ReadLoopAsync(CancellationToken ct)
    {
        var header = new byte[4];
        try
        {
            while (!ct.IsCancellationRequested && _stream is { CanRead: true })
            {
                await ReadExactlyAsync(_stream, header, ct);
                uint len = BinaryPrimitives.ReadUInt32LittleEndian(header);
                var payload = new byte[len];
                await ReadExactlyAsync(_stream, payload, ct);

                OnMessage(payload);
            }
        }
        catch (OperationCanceledException) { }
        catch (IOException) { State = ConnectionState.Disconnected; }
    }

    private void OnMessage(byte[] data)
    {
        if (data.Length < 2) return;
        ushort kind = BitConverter.ToUInt16(data, 0);

        OnAnyMessage?.Invoke(kind, data);

        switch (kind)
        {
            case 5: // ReducerResult
                if (data.Length >= 11)
                {
                    long reqId = BitConverter.ToInt64(data, 10);
                    if (_pendingCalls.TryRemove(reqId, out var tcs))
                        tcs.TrySetResult(null);
                }
                break;
        }
    }

    private static async Task ReadExactlyAsync(NetworkStream stream, byte[] buffer, CancellationToken ct)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset), ct);
            if (read == 0) throw new IOException("connection closed");
            offset += read;
        }
    }

    private static uint Crc32(byte[] data)
    {
        uint crc = 0xFFFFFFFF;
        foreach (byte b in data)
        {
            crc ^= b;
            for (int i = 0; i < 8; i++)
                crc = (crc & 1) != 0 ? (crc >> 1) ^ 0xEDB88320 : crc >> 1;
        }
        return ~crc;
    }

    public void Dispose() { _cts?.Cancel(); _stream?.Dispose(); _tcp?.Dispose(); }
}

// ─── supporting types ─────────────────────────────────────────────────────

public enum ConnectionState { Disconnected, Connecting, Connected, Authenticated }

public sealed class TableView
{
    private readonly ConcurrentDictionary<ulong, Dictionary<string, object?>> _rows = new();
    private readonly int _limit;

    public event Action<Dictionary<string, object?>>? OnInsert;
    public event Action<Dictionary<string, object?>>? OnUpdate;
    public event Action<ulong>? OnDelete;

    internal TableView(string table, int limit) { TableName = table; _limit = limit; }
    public string TableName { get; }

    public Dictionary<string, object?>? Get(ulong rowId) =>
        _rows.TryGetValue(rowId, out var row) ? row : null;

    public List<Dictionary<string, object?>> Rows() => _rows.Values.ToList();

    internal void ApplyInsert(ulong id, Dictionary<string, object?> row)
    { _rows[id] = row; OnInsert?.Invoke(row); }

    internal void ApplyUpdate(ulong id, Dictionary<string, object?> row)
    { _rows[id] = row; OnUpdate?.Invoke(row); }

    internal void ApplyDelete(ulong id)
    { _rows.TryRemove(id, out _); OnDelete?.Invoke(id); }
}
