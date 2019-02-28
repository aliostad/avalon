using System;
using System.Collections.Generic;
using System.Text;
using Spreads.LMDB;
using Spreads.Buffers;

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
    }
}
