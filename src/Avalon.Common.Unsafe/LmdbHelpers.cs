using Spreads.LMDB;
using Spreads.Buffers;
using Spreads;
using System.Runtime.CompilerServices;
using System;

namespace Avalon.Common
{
    public static class LmdbHelpers
    {
        public static unsafe void Put(this Transaction tx, Database db, Bufferable key, Bufferable value,
            TransactionPutOptions flags = TransactionPutOptions.None)
        {
            fixed (byte* keyPtr = &key.Buffer[0], valPtr = &value.Buffer[0])
            {
                var keydb = new DirectBuffer(key.Buffer.Length, keyPtr);
                var valuedb = new DirectBuffer(value.Buffer.Length, valPtr);
                db.Put(tx, ref keydb, ref valuedb, flags);
            }
        }

        public static unsafe bool TryGet(this ReadOnlyTransaction tx, Database db, Bufferable key, out Bufferable value)
        {
            value = default;
            fixed (byte* keyPtr = &key.Buffer[0])
            {
                var keydb = new DirectBuffer(key.Buffer.Length, keyPtr);
                DirectBuffer valuedb = default;
                var success = db.TryGet(tx, ref keydb, out valuedb);
                if (success)
                    value = new Bufferable(valuedb.Span.ToArray());

                return success;
            }

        }

        public static unsafe bool TryGetDuplicate(this ReadOnlyTransaction tx, Database db, Bufferable key, ref Bufferable value)
        {
            using (var c = db.OpenReadOnlyCursor(tx))
            {
                fixed (byte* keyPtr = &key.Buffer[0], valPtr = &value.Buffer[0])
                {
                    var keydb = new DirectBuffer(key.Buffer.Length, keyPtr);
                    var valuedb = new DirectBuffer(value.Buffer.Length, valPtr);

                    var success = c.TryFindDup(Lookup.EQ, ref keydb, ref valuedb);
                    if (success)
                        value = new Bufferable(valuedb.Span.ToArray());

                    return success;
                }

            }
        }


        /// <summary>
        /// Deletes up to (but not including) this value for the key.
        /// </summary>
        /// <param name="tx">transaction</param>
        /// <param name="db">database</param>
        /// <param name="key">key</param>
        /// <param name="value">Value (or part of the value) to which the data will be deleted</param>
        /// <param name="firstValue">First value to position to the beginning of dupes. Even if first value is bigger it is OK.</param>
        /// <returns></returns>
        public static unsafe bool DeleteUpToValue(this Transaction tx, Database db, long key, long value, long firstValue = 0L)
        {
            using (var c = db.OpenCursor(tx))
            {
                var keydb = new DirectBuffer(BitConverter.GetBytes(key));
                var valdb = new DirectBuffer(BitConverter.GetBytes(firstValue));
                c.TryFindDup(Lookup.EQ, ref keydb, ref valdb);

                while (c.Delete(false))
                {
                    try
                    {
                        if(!c.TryGet(ref keydb, ref valdb, CursorGetOption.GetCurrent) || valdb.IsEmpty)
                            break; // empty now
                    }
                    catch (LMDBException e)
                    {
                        if (e.Message.StartsWith("Invalid argument"))
                                break; // empty database now - it is a bug will be fixed
                            else
                                throw;
                    }

                        var currentValue = valdb.ReadInt64(0);
                        if (currentValue == value)
                            break;

                }

                return true;
            }
        }
    }
}
