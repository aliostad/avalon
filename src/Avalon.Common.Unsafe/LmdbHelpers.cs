using Spreads.LMDB;
using Spreads.Buffers;
using Spreads;
using System.Runtime.CompilerServices;
using System;
using Avalon.Raft.Core.Persistence;

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
            int deletedCount = 0;
            using (var c = db.OpenCursor(tx))
            {
                try
                {
                    byte* keyptr = (byte*) Unsafe.AsPointer(ref key);
                    byte* valptr = (byte*) Unsafe.AsPointer(ref value);
                    byte* firstvalptr = (byte*) Unsafe.AsPointer(ref firstValue);
                    var keydb = new DirectBuffer(sizeof(long), keyptr);
                    var valdb = new DirectBuffer(sizeof(long), firstvalptr);
                     c.TryFindDup(Lookup.LE, ref keydb, ref valdb);

                    while (c.Delete(false))
                    {
                        deletedCount++;
                        if(!c.TryGet(ref keydb, ref valdb, CursorGetOption.GetBothRange) || valdb.IsEmpty)
                            break; // empty now

                        var currentValue = valdb.ReadInt64(0);
                        if (currentValue == value)
                            break;

                    }

                    return true;
                }
                catch(Exception e)
                {
                    throw new DeleteUpToException(
                        $"Failed after deleting {deletedCount} records."
                        , e);
                }
            }
        }
    }
}
