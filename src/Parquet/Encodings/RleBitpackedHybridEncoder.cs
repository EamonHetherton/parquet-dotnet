using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using Parquet.Extensions;

namespace Parquet.Encodings {
    static partial class RleBitpackedHybridEncoder {

        private static readonly ArrayPool<byte> BytePool = ArrayPool<byte>.Shared;
        private const int MaxValueCount = int.MaxValue >> 1;  //max count for an integer with one lost bit

        /// <summary>
        /// Writes to target stream without jumping around, therefore can be used in forward-only stream.
        /// Before writing actual data, writes out int32 value indicating total data binary length.
        /// </summary>
        public static void EncodeWithLength(Stream s, int bitWidth, Span<int> data) {

            int length = Encode(data, bitWidth, out byte[]? rentedBuffer);
            if(rentedBuffer == null)
                return;
            try {
                s.WriteInt32(length);
                s.Write(rentedBuffer, 0, length);
            } finally {
                BytePool.Return(rentedBuffer);
            }
        }

        public static void Encode(Stream dest, Span<int> data, int bitWidth) {
            int length = Encode(data, bitWidth, out byte[]? rentedBuffer);
            if(rentedBuffer == null)
                return;
            try {
                dest.Write(rentedBuffer, 0, length);
            } finally {
                BytePool.Return(rentedBuffer);
            }
        }
        public static void Encode(Stream dest, Span<bool> data) {

            int byteWidth = 1;
            if(byteWidth > 4)
                throw new IOException($"encountered bit width ({byteWidth}) that requires more than 4 bytes.");

            // rent necessary buffer of max length
            var rentedBuffer = BytePool.Rent((byteWidth + 4) * data.Length);
            int consumed = 0;

            try {
                Encode1(rentedBuffer, ref consumed, data);
            } catch {
                BytePool.Return(rentedBuffer);
                rentedBuffer = null;
                throw;
            }

            //return consumed;


            //int length = Encode(data, out byte[]? rentedBuffer);
            //if(rentedBuffer == null)
            //    return;
            dest.Write(rentedBuffer, 0, consumed);
        }
        private static void Encode1(byte[] s, ref int consumed, Span<bool> data) {

            //chunk identical values and write
            bool lastValue = false;
            int chunkCount = 0;
            for(int i = 0; i < data.Length; i++) {
                bool item = data[i];

                if(chunkCount == 0) {
                    chunkCount = 1;
                    lastValue = item;
                } else if(item != lastValue || chunkCount == MaxValueCount) {
                    WriteRle1(s, ref consumed, chunkCount, lastValue ? 1 : 0);

                    chunkCount = 1;
                    lastValue = item;
                } else
                    chunkCount += 1;
            }

            if(chunkCount > 0)
                WriteRle1(s, ref consumed, chunkCount, lastValue ? 1 : 0);
        }


        public static int Encode(Span<bool> data, out byte[]? buffer) {
            var enc = new RunLengthBitPackingHybridEncoder(1);
            for(int i = 0; i < data.Length; i++) {

                var value = data[i];
                enc.writeBool(value);
            }
            buffer = enc.toBytes();
            return buffer.Length;
        }

        public static int Encode(Span<int> data, int bitWidth, out byte[]? rentedBuffer) {
            var ussenew = true;
            if(ussenew) {
                var enc = new RunLengthBitPackingHybridEncoder(bitWidth);
                for(int i = 0; i < data.Length; i++) {

                    var value = data[i];
                    enc.writeInt(value);
                }
                var result = enc.toBytes();

                rentedBuffer = BytePool.Rent(result.Length);
                Array.Copy(result, rentedBuffer, result.Length);
                return result.Length;
            } else {

                //for simplicity, we're only going to write RLE, however bitpacking needs to be implemented as well

                int byteWidth = (bitWidth + 7) / 8; //number of whole bytes for this bit width
                if(byteWidth > 4)
                    throw new IOException($"encountered bit width ({byteWidth}) that requires more than 4 bytes.");

                // rent necessary buffer of max length
                rentedBuffer = BytePool.Rent((byteWidth + 4) * data.Length);
                int consumed = 0;

                try {
                    switch(byteWidth) {
                        case 0:
                            Encode0(rentedBuffer, ref consumed, data);
                            break;
                        case 1:
                            Encode1(rentedBuffer, ref consumed, data);
                            break;
                        case 2:
                            Encode2(rentedBuffer, ref consumed, data);
                            break;
                        case 3:
                            Encode3(rentedBuffer, ref consumed, data);
                            break;
                        case 4:
                            Encode4(rentedBuffer, ref consumed, data);
                            break;
                    }
                } catch {
                    BytePool.Return(rentedBuffer);
                    rentedBuffer = null;
                    throw;
                }

                return consumed;
            }
        }

        private static void EncodeBitPacked(byte[] s, ref int consumed, Span<int> data, int bitwidth) {

            //chunk identical values and write
            int nextBitIndex = 0;
            int lastValue = 0;
            int chunkCount = 0;
            for(int i = 0; i < data.Length; i++) {
                int item = data[i];

                if(chunkCount == 0) {
                    chunkCount = 1;
                    lastValue = item;
                } else if(item != lastValue || chunkCount == MaxValueCount) {
                    WriteRleBitPacked(s, ref consumed, ref nextBitIndex, chunkCount, lastValue, bitwidth);

                    chunkCount = 1;
                    lastValue = item;
                } else
                    chunkCount += 1;
            }

            if(chunkCount > 0)
                WriteRleBitPacked(s, ref consumed, ref nextBitIndex, chunkCount, lastValue, bitwidth);
        }
        private static void WriteRleBitPacked(byte[] r, ref int consumed, ref int bitIndex, int rle_run_len, int value, int bitWidth) {

            int rle_header = rle_run_len << 1;
            ((ulong)rle_header).ULEB128Encode(r, ref consumed);

            switch(bitIndex) {
                case 0:
                    r[consumed++] = (byte)value;
                    break;
                case 1:
                    r[consumed] = (byte)(r[consumed] | (value >> 1));

                    if(bitWidth > 7) {
                        r[consumed] = (byte)(r[consumed] | (value >> 1));
                    }
                    break;
            }
            bitIndex += bitWidth;
        }

        private static void WriteRle0(byte[] r, ref int consumed, int chunkCount, int value) {
            int header = chunkCount << 1;
            ((ulong)header).ULEB128Encode(r, ref consumed);
        }

        private static void WriteRle1(byte[] r, ref int consumed, int chunkCount, int value) {
            int header = chunkCount << 1;
            ((ulong)header).ULEB128Encode(r, ref consumed);
            r[consumed++] = (byte)value;
        }

        private static void WriteRle2(byte[] r, ref int consumed, int chunkCount, int value) {
            int header = chunkCount << 1;
            ((ulong)header).ULEB128Encode(r, ref consumed);
            r[consumed++] = (byte)value;
            r[consumed++] = (byte)((value >> 8) & 0xFF);
        }

        private static void WriteRle3(byte[] r, ref int consumed, int chunkCount, int value) {
            int header = chunkCount << 1;
            ((ulong)header).ULEB128Encode(r, ref consumed);
            r[consumed++] = (byte)value;
            r[consumed++] = (byte)((value >> 8) & 0xFF);
            r[consumed++] = (byte)((value >> 16) & 0x00FF);
        }

        private static void WriteRle4(byte[] r, ref int consumed, int chunkCount, int value) {
            int header = chunkCount << 1;
            ((ulong)header).ULEB128Encode(r, ref consumed);
            r[consumed++] = (byte)value;
            r[consumed++] = (byte)((value >> 8) & 0xFF);
            r[consumed++] = (byte)((value >> 16) & 0x00FF);
            r[consumed++] = (byte)((value >> 32) & 0x0000FF);
        }

        public static int Decode(Span<byte> s,
            int bitWidth,
            int? spanLength,
            out int usedSpanLength,
            Span<int> dest, int pageSize) {

            int length;
            if(spanLength == null) {
                length = s.ReadInt32(0);
                s = s.Slice(sizeof(int), length);
                usedSpanLength = length + sizeof(int);
            } else {
                length = spanLength.Value;
                s = s.Slice(0, length);
                usedSpanLength = length;
            }

            return Decode(s, bitWidth, dest, pageSize);
        }

        /* from specs:
         * rle-bit-packed-hybrid: <length> <encoded-data>
         * length := length of the <encoded-data> in bytes stored as 4 bytes little endian
         * encoded-data := <run>*
         * run := <bit-packed-run> | <rle-run>  
         * bit-packed-run := <bit-packed-header> <bit-packed-values>  
         * bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)  
         * // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8  
         * bit-pack-count := (number of values in this run) / 8  
         * bit-packed-values := *see 1 below*  
         * rle-run := <rle-header> <repeated-value>  
         * rle-header := varint-encode( (number of times repeated) << 1)  
         * repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
         */

        public static int Decode(Span<byte> data, int bitWidth, Span<int> dest, int pageSize) {
            int dataOffset = 0;

            int byteWidth = (bitWidth + 7) / 8; //round up to next byte
            int destOffset = 0;
            while(dataOffset < data.Length) {
                int header = (int)data.ULEB128Decode(ref dataOffset);
                //int header = (int)data.ULEB128Decode(ref dataOffset);
                bool isRle = (header & 1) == 0;

                if(isRle)
                    destOffset += ReadRle(header, data, ref dataOffset, byteWidth, dest.Slice(destOffset), pageSize - destOffset);
                else
                    destOffset += DecodeBitpacked(header, data, ref dataOffset, bitWidth, dest.Slice(destOffset), pageSize - destOffset);
            }

            return destOffset;
        }

        private static int ReadRle(int header, Span<byte> data, ref int offset, int byteWidth, Span<int> dest, int maxItems) {
            // The count is determined from the header and the width is used to grab the
            // value that's repeated. Yields the value repeated count times.

            int destOffset = 0;
            int headerCount = header >> 1;
            if(headerCount == 0)
                return 0; // important not to continue reading as will result in data corruption in data page further
            int count = Math.Min(headerCount, maxItems); // make sure we remain within bounds
            int value = ReadIntOnBytes(data.Slice(offset, byteWidth));
            offset += byteWidth;

            for(int i = 0; i < count; i++)
                dest[destOffset++] = value;

            return destOffset;
        }

        internal static int DecodeBitpacked(int header, Span<byte> data, ref int dataOffset,
            int bitWidth,
            Span<int> dest, int maxItems) {

            int unpacked = 0;
            int numGroups = header >> 1;

            if(numGroups == 0)
                return 0;

            int count = numGroups * 8;
            int bytesToRead = (int)Math.Ceiling(bitWidth * count / 8.0);
            bytesToRead = Math.Min(bytesToRead, data.Length - dataOffset);
            Span<byte> rawSpan = data.Slice(dataOffset, bytesToRead);
            dataOffset += bytesToRead;

            for(int valueIndex = 0, byteIndex = 0; valueIndex < count; valueIndex += 8, byteIndex += bitWidth) {
                unpacked += BitPackedEncoder.Decode8ValuesLE(rawSpan.Slice(byteIndex), dest.Slice(valueIndex), bitWidth);
            }

            return Math.Min(unpacked, maxItems);
        }

        private static int ReadIntOnBytes(Span<byte> data) {
            switch(data.Length) {
                case 0:
                    return 0;
                case 1:
                    return data[0];
                case 2:
                    return (data[1] << 8) + data[0];
                case 3:
                    return (data[2] << 16) + (data[1] << 8) + data[0];
                case 4:
                    return BitConverter.ToInt32(data.ToArray(), 0);
                default:
                    throw new IOException($"encountered byte width ({data.Length}) that requires more than 4 bytes.");
            }
        }
    }


    //public interface IBytePacker {
    //    void pack8Values(int[] input, in int inPos, byte[] output, int outPos);
    //}

    //public static class BytPackerFactory {
    //    public static IBytePacker Create(int bitWidth) {
    //        switch(bitWidth) {
    //            case 1:
    //                return new BytePacker1();
    //            case 2:
    //                return new BytePacker2();
    //            case 3:
    //                return new BytePacker3();
    //            case 4:
    //                return new BytePacker4();
    //            case 6:
    //                return new BytePacker6();
    //            case 7:
    //                return new BytePacker7();
    //            case 8:
    //                return new BytePacker8();
    //            case 9:
    //                return new BytePacker9();
    //            case 10:
    //                return new BytePacker10();
    //            case 11:
    //                return new BytePacker11();
    //            case 15:
    //                return new BytePacker15();
    //        }
    //        return new BytePacker(bitWidth);
    //    }
    //}
    //public class BytePacker1 : IBytePacker {
    //    private const int mask = (1 << 1) - 1;
    //    private const int bitWidth = 1;
    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {
    //        if(inPos > 0) {
    //            input = input[inPos..8];
    //        }
    //        var value = (input[0] & mask) | ((input[1] & mask) << bitWidth)
    //            | ((input[2] & mask) << (bitWidth * 2))
    //            | ((input[3] & mask) << (bitWidth * 3))
    //            | ((input[4] & mask) << (bitWidth * 4))
    //            | ((input[5] & mask) << (bitWidth * 5))
    //            | ((input[6] & mask) << (bitWidth * 6))
    //            | ((input[7] & mask) << (bitWidth * 7));

    //        output[outPos] = (byte)(value & 0x000000FF);
    //    }
    //}
    //public class BytePacker2 : IBytePacker {
    //    private const int mask = (1 << 2) - 1;
    //    private const int bitWidth = 2;
    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {
    //        if(inPos > 0) {
    //            input = input[inPos..8];
    //        }
    //        var value = (input[0] & mask) | ((input[1] & mask) << bitWidth)
    //            | ((input[2] & mask) << (bitWidth * 2))
    //            | ((input[3] & mask) << (bitWidth * 3))
    //            | ((input[4] & mask) << (bitWidth * 4))
    //            | ((input[5] & mask) << (bitWidth * 5))
    //            | ((input[6] & mask) << (bitWidth * 6))
    //            | ((input[7] & mask) << (bitWidth * 7));

    //        output[outPos] = (byte)(value & 0x000000FF);
    //        output[outPos + 1] = (byte)((value >> 8) & 0x000000FF);
    //    }
    //}
    //public class BytePacker3 : IBytePacker {
    //    private const int mask = (1 << 3) - 1;
    //    private const int bitWidth = 3;
    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {
    //        if(inPos > 0) {
    //            input = input[inPos..8];
    //        }
    //        var value = (input[0] & mask) | ((input[1] & mask) << bitWidth)
    //            | ((input[2] & mask) << (bitWidth * 2))
    //            | ((input[3] & mask) << (bitWidth * 3))
    //            | ((input[4] & mask) << (bitWidth * 4))
    //            | ((input[5] & mask) << (bitWidth * 5))
    //            | ((input[6] & mask) << (bitWidth * 6))
    //            | ((input[7] & mask) << (bitWidth * 7));

    //        output[outPos] = (byte)(value & 0x000000FF);
    //        output[outPos + 1] = (byte)((value >> 8) & 0x000000FF);
    //        output[outPos + 2] = (byte)((value >> 16) & 0x000000FF);
    //    }
    //}
    //public class BytePacker4 : IBytePacker {
    //    private const int mask = (1 << 4) - 1;
    //    private const int bitWidth = 4;
    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {
    //        if(inPos > 0) {
    //            input = input[inPos..8];
    //        }
    //        var value = (input[0] & mask) | ((input[1] & mask) << bitWidth)
    //            | ((input[2] & mask) << (bitWidth * 2))
    //            | ((input[3] & mask) << (bitWidth * 3))
    //            | ((input[4] & mask) << (bitWidth * 4))
    //            | ((input[5] & mask) << (bitWidth * 5))
    //            | ((input[6] & mask) << (bitWidth * 6))
    //            | ((input[7] & mask) << (bitWidth * 7));

    //        output[outPos] = (byte)(value & 0x000000FF);
    //        output[outPos + 1] = (byte)((value >> 8) & 0x000000FF);
    //        output[outPos + 2] = (byte)((value >> 16) & 0x000000FF);
    //        output[outPos + 3] = (byte)((value >> 24) & 0x000000FF);
    //    }
    //}
    //public class BytePacker6 : IBytePacker {
    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {
    //        if(inPos > 0) {
    //            input = input[inPos..8];
    //        }
    //        //0: 00000000 00000000
    //        //1: 00000000 00111111
    //        //2: 00000000 00000000
    //        //3: 00000000 00111111
    //        //4: 00000000 00000000
    //        //5: 00000000 00111111
    //        //6: 00000000 00000000
    //        //7: 00000000 00111111
    //        //11111100 00001111 11000000 11111100 00001111 11000000
    //        output[outPos] = (byte)(input[0] | (input[1] << 6));
    //        output[outPos + 1] = (byte)((input[1] >> 2) | (input[2] << 4));
    //        output[outPos + 2] = (byte)((input[2] >> 4) | (input[3] << 2));
    //        output[outPos + 3] = (byte)((input[4]) | (input[5] << 6));
    //        output[outPos + 4] = (byte)((input[5] >> 2) | (input[6] << 4));
    //        output[outPos + 5] = (byte)((input[6] >> 4) | (input[7] << 2));

    //    }
    //}
    //public class BytePacker7 : IBytePacker {
    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {
    //        if(inPos > 0) {
    //            input = input[inPos..8];
    //        }
    //        //0: 00000000 00000000
    //        //1: 00000000 01111111
    //        //2: 00000000 00000000
    //        //3: 00000000 01111111
    //        //4: 00000000 00000000
    //        //5: 00000000 01111111
    //        //6: 00000000 00000000
    //        //7: 00000000 01111111
    //        //11111110 00000011 11111000 00001111 11100000 00111111 10000000
    //        output[outPos] = (byte)(input[0] | (input[1] << 7));
    //        output[outPos + 1] = (byte)((input[1] >> 1) | (input[2] << 6));
    //        output[outPos + 2] = (byte)((input[2] >> 2) | (input[3] << 5));
    //        output[outPos + 3] = (byte)((input[3] >> 3) | (input[4] << 4));
    //        output[outPos + 4] = (byte)((input[4] >> 4) | (input[5] << 3));
    //        output[outPos + 5] = (byte)((input[5] >> 5) | (input[6] << 2));
    //        output[outPos + 6] = (byte)((input[6] >> 6) | (input[7] << 1));

    //    }
    //}
    //public class BytePacker8 : IBytePacker {
    //    private const int mask = (1 << 8) - 1;
    //    private const int bitWidth = 8;
    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {
    //        if(inPos > 0) {
    //            input = input[inPos..8];
    //        }
    //        output[outPos] = (byte)input[0];
    //        output[outPos + 1] = (byte)input[1];
    //        output[outPos + 2] = (byte)input[2];
    //        output[outPos + 3] = (byte)input[3];
    //        output[outPos + 4] = (byte)input[4];
    //        output[outPos + 5] = (byte)input[5];
    //        output[outPos + 6] = (byte)input[6];
    //        output[outPos + 7] = (byte)input[7];
    //    }
    //}

    //public class BytePacker9 : IBytePacker {
    //    private const int mask = (1 << 9) - 1;
    //    private const int bitWidth = 9;
    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {
    //        if(inPos > 0) {
    //            input = input[inPos..8];
    //        }
    //        output[outPos] = (byte)input[0];
    //        output[outPos + 1] = (byte)((input[0] >> 8) | (input[1] << 1));
    //        output[outPos + 2] = (byte)((input[1] >> 7) | (input[2] << 2));
    //        output[outPos + 3] = (byte)((input[2] >> 6) | (input[3] << 3));
    //        output[outPos + 4] = (byte)((input[3] >> 5) | (input[4] << 4));
    //        output[outPos + 5] = (byte)((input[4] >> 4) | (input[5] << 5));
    //        output[outPos + 6] = (byte)((input[5] >> 3) | (input[6] << 6));
    //        output[outPos + 7] = (byte)((input[6] >> 2) | (input[7] << 7));
    //        output[outPos + 8] = (byte)(input[7] >> 1);
    //    }
    //}
    //public class BytePacker10 : IBytePacker {
    //    //private const int mask = (1 << 9) - 1;
    //    //private const int bitWidth = 9;
    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {
    //        if(inPos > 0) {
    //            input = input[inPos..8];
    //        }
    //        //0: 00000000 00000000
    //        //1: 00000011 11111111
    //        //2: 00000000 00000000
    //        //3: 00000011 11111111
    //        //4: 00000000 00000000
    //        //5: 00000011 11111111
    //        //6: 00000000 00000000
    //        //7: 00000011 11111111
    //        //11111111 11000000 00001111 11111100 00000000 11111111 11000000 00001111 11111100 00000000

    //        output[outPos] = (byte)input[0];
    //        output[outPos + 1] = (byte)((input[0] >> 8) | (input[1] << 2));
    //        output[outPos + 2] = (byte)((input[1] >> 6) | (input[2] << 4));
    //        output[outPos + 3] = (byte)((input[2] >> 4) | (input[3] << 6));
    //        output[outPos + 4] = (byte)(input[3] >> 2);
    //        output[outPos + 5] = (byte)input[4];
    //        output[outPos + 6] = (byte)((input[4] >> 8) | (input[5] << 2));
    //        output[outPos + 7] = (byte)((input[5] >> 6) | (input[6] << 4));
    //        output[outPos + 8] = (byte)((input[6] >> 4) | (input[7] << 6));
    //        output[outPos + 9] = (byte)(input[7] >> 2);
    //    }
    //}
    //public class BytePacker11 : IBytePacker {
    //    //private const int mask = (1 << 9) - 1;
    //    //private const int bitWidth = 9;
    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {
    //        if(inPos > 0) {
    //            input = input[inPos..8];
    //        }
    //        //0: 00000000 00000000
    //        //1: 00000111 11111111
    //        //2: 00000000 00000000
    //        //3: 00000111 11111111
    //        //4: 00000000 00000000
    //        //5: 00000111 11111111
    //        //6: 00000000 00000000
    //        //7: 00000111 11111111
    //        //11111111 11100000 00000011 11111111 10000000 00001111 11111110 00000000 00111111 11111000 00000000

    //        output[outPos] = (byte)input[0];
    //        output[outPos + 1] = (byte)((input[0] >> 8) | (input[1] << 3));
    //        output[outPos + 2] = (byte)((input[1] >> 5) | (input[2] << 6));
    //        output[outPos + 3] = (byte)(input[2] >> 2);
    //        output[outPos + 4] = (byte)((input[2] >> 10) | (input[3] << 1));
    //        output[outPos + 5] = (byte)((input[3] >> 7) | (input[4] << 4));
    //        output[outPos + 6] = (byte)((input[4] >> 4) | (input[5] << 7));
    //        output[outPos + 7] = (byte)(input[5] >> 1);
    //        output[outPos + 8] = (byte)((input[5] >> 9) | (input[6] << 2));
    //        output[outPos + 9] = (byte)((input[6] >> 6) | (input[7] << 5));
    //        output[outPos + 10] = (byte)(input[7] >> 3);
    //    }
    //}
    //public class BytePacker15 : IBytePacker {
    //    private const int mask = (1 << 15) - 1;
    //    private const int bitWidth = 15;
    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {
    //        if(inPos > 0) {
    //            input = input[inPos..8];
    //        }
    //        //00000000 00000000
    //        //01111111 11111111
    //        //00000000 00000000
    //        //01111111 11111111
    //        //00000000 00000000
    //        //01111111 11111111
    //        //00000000 00000000
    //        //01111111 11111111
    //        //11111111 11111110 00000000 00000011 11111111 11111000 00000000 00001111 11111111 11100000 00000000 00111111 11111111 10000000 00000000

    //        output[outPos] = (byte)input[0];
    //        output[outPos + 1] = (byte)((input[0] >> 8) | (input[1] << 7));
    //        output[outPos + 2] = (byte)(input[1] >> 1);
    //        output[outPos + 3] = (byte)((input[1] >> 9) | (input[2] << 6));
    //        output[outPos + 4] = (byte)(input[2] >> 2);
    //        output[outPos + 5] = (byte)((input[2] >> 10) | (input[3] << 5));
    //        output[outPos + 6] = (byte)(input[3] >> 3);
    //        output[outPos + 7] = (byte)((input[3] >> 11) | (input[4] << 4));
    //        output[outPos + 8] = (byte)(input[4] >> 4);
    //        output[outPos + 9] = (byte)((input[4] >> 12) | (input[5] << 3));
    //        output[outPos + 10] = (byte)(input[5] >> 5);
    //        output[outPos + 11] = (byte)((input[5] >> 13) | (input[6] << 2));
    //        output[outPos + 12] = (byte)(input[6] >> 6);
    //        output[outPos + 13] = (byte)((input[6] >> 14) | (input[7] << 1));
    //        output[outPos + 14] = (byte)(input[7] >> 7);
    //    }
    //}

    //public class BytePacker : IBytePacker {
    //    private const int NUM_OF_INTS = 8;

    //    private readonly int _width;

    //    public BytePacker(int width) {
    //        this._width = width;
    //    }

    //    public void pack8Values(int[] input, in int inPos, byte[] output, int outPos) {

    //        int valueIdx = inPos;
    //        // remaining bits for the current unfinished Integer
    //        int leftBit = 0;

    //        while(valueIdx < NUM_OF_INTS + inPos) {
    //            // buffer is used for saving 32 bits as a part of result
    //            int buffer = 0;
    //            // remaining size of bits in the 'buffer'
    //            int leftSize = 32;

    //            // encode the left bits of current Integer to 'buffer'
    //            if(leftBit > 0) {
    //                buffer |= (input[valueIdx] << (32 - leftBit));
    //                leftSize -= leftBit;
    //                leftBit = 0;
    //                valueIdx++;
    //            }

    //            while(leftSize >= _width && valueIdx < NUM_OF_INTS + inPos) {
    //                // encode one Integer to the 'buffer'
    //                buffer |= (input[valueIdx] << (leftSize - _width));
    //                leftSize -= _width;
    //                valueIdx++;
    //            }
    //            // If the remaining space of the buffer can not save the bits for one Integer,
    //            if(leftSize > 0 && valueIdx < NUM_OF_INTS + inPos) {
    //                // put the first 'leftSize' bits of the Integer into remaining space of the
    //                // buffer
    //                buffer |= (input[valueIdx] >>> (_width - leftSize));
    //                leftBit = _width - leftSize;
    //            }

    //            // put the buffer into the final result
    //            for(int j = 0; j < 4; j++) {
    //                output[outPos] = (byte)((buffer >>> ((3 - j) * 8)) & 0xFF);
    //                outPos++;
    //                if(outPos >= _width) {
    //                    return;
    //                }
    //            }
    //        }
    //    }
    //}
    internal class RunLengthBitPackingHybridEncoder {
        public RunLengthBitPackingHybridEncoder(int bitWidth)//, int initialCapacity, int pageSize)//, ByteBufferAllocator allocator)
        {


            //Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");

            this.bitWidth = bitWidth;
            this.byteWidth = (bitWidth + 7) / 8; //number of whole bytes for this bit width
            this.baos = new();// CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator);
            this.packBuffer = new byte[bitWidth];
            this.bufferedValues = new int[8];
            //this.packer = BytPackerFactory.Create(bitWidth);// Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
            reset(false);
        }

        //private readonly IBytePacker packer;

        private readonly List<byte> baos;// CapacityByteArrayOutputStream baos;

        /**
         * The bit width used for bit-packing and for writing
         * the repeated-value
         */
        private readonly int bitWidth;
        private readonly int byteWidth;

        /**
         * Values that are bit packed 8 at at a time are packed into this
         * buffer, which is then written to baos
         */
        private readonly byte[] packBuffer;

        /**
         * Previous value written, used to detect repeated values
         */
        private int previousValue;

        /*
         * We buffer 8 values at a time, and either bit pack them
         * or discard them after writing a rle-run
         */
        private readonly int[] bufferedValues;

        private int numBufferedValues;

        /**
         * How many times a value has been repeated
         */
        private int repeatCount;

        /**
         * How many groups of 8 values have been written
         * to the current bit-packed-run
         */
        private int bitPackedGroupCount;

        /*
         * A "pointer" to a single byte in baos,
         * which we use as our bit-packed-header. It's really
         * the logical index of the byte in baos.
         * <p>
         * We are only using one byte for this header,
         * which limits us to writing 504 values per bit-packed-run.
         * <p>
         * MSB must be 0 for varint encoding, LSB must be 1 to signify
         * that this is a bit-packed-header leaves 6 bits to write the
         * number of 8-groups -> (2^6 - 1) * 8 = 504
         */
        private int bitPackedRunHeaderPointer;

        private bool toBytesCalled;



        private void reset(bool resetBaos) {
            if(resetBaos) {
                this.baos.Clear();
                //this.baos.reset();
            }
            this.previousValue = 0;
            this.numBufferedValues = 0;
            this.repeatCount = 0;
            this.bitPackedGroupCount = 0;
            this.bitPackedRunHeaderPointer = -1;
            this.toBytesCalled = false;
        }

        public void writeInt(int value) {
            if(value == previousValue) {
                // keep track of how many times we've seen this value
                // consecutively
                ++repeatCount;

                if(repeatCount >= 8) {
                    // we've seen this at least 8 times, we're
                    // certainly going to write an rle-run,
                    // so just keep on counting repeats for now
                    return;
                }
            } else {
                // This is a new value, check if it signals the end of
                // an rle-run
                if(repeatCount >= 8) {
                    // it does! write an rle-run
                    writeRleRun();
                }

                // this is a new value so we've only seen it once
                repeatCount = 1;
                // start tracking this value for repeats
                previousValue = value;
            }

            // We have not seen enough repeats to justify an rle-run yet,
            // so buffer this value in case we decide to write a bit-packed-run
            bufferedValues[numBufferedValues] = value;
            ++numBufferedValues;

            if(numBufferedValues == 8) {
                // we've encountered less than 8 repeated values, so
                // either start a new bit-packed-run or append to the
                // current bit-packed-run
                writeOrAppendBitPackedRun();
            }
        }
        public void writeBool(bool b) {
            var value = b ? 1 : 0;
            if(value == previousValue) {
                // keep track of how many times we've seen this value
                // consecutively
                ++repeatCount;

                if(repeatCount >= 8) {
                    // we've seen this at least 8 times, we're
                    // certainly going to write an rle-run,
                    // so just keep on counting repeats for now
                    return;
                }
            } else {
                // This is a new value, check if it signals the end of
                // an rle-run
                if(repeatCount >= 8) {
                    // it does! write an rle-run
                    writeRleRun();
                }

                // this is a new value so we've only seen it once
                repeatCount = 1;
                // start tracking this value for repeats
                previousValue = value;
            }

            // We have not seen enough repeats to justify an rle-run yet,
            // so buffer this value in case we decide to write a bit-packed-run
            bufferedValues[numBufferedValues] = value;
            ++numBufferedValues;

            if(numBufferedValues == 8) {
                // we've encountered less than 8 repeated values, so
                // either start a new bit-packed-run or append to the
                // current bit-packed-run
                writeOrAppendBitPackedRun();
            }
        }


        private void writeOrAppendBitPackedRun() {
            if(bitPackedGroupCount >= 63) {
                // we've packed as many values as we can for this run,
                // end it and start a new one
                endPreviousBitPackedRun();
            }

            if(bitPackedRunHeaderPointer == -1) {
                // this is a new bit-packed-run, allocate a byte for the header
                // and keep a "pointer" to it so that it can be mutated later
                baos.Add(0);// baos.write(0); // write a sentinel value
                bitPackedRunHeaderPointer = baos.Count - 1;// baos.getCurrentIndex();
            }

            BitPackedEncoder.Pack8ValuesLE(bufferedValues, packBuffer, bitWidth);
            //packer.pack8Values(bufferedValues, 0, packBuffer, 0);
            baos.AddRange(packBuffer);

            // empty the buffer, they've all been written
            numBufferedValues = 0;

            // clear the repeat count, as some repeated values
            // may have just been bit packed into this run
            repeatCount = 0;

            ++bitPackedGroupCount;
        }

        /*
         * If we are currently writing a bit-packed-run, update the
         * bit-packed-header and consider this run to be over
         * <p>
         * does nothing if we're not currently writing a bit-packed run
         */
        private void endPreviousBitPackedRun() {
            if(bitPackedRunHeaderPointer == -1) {
                // we're not currently in a bit-packed-run
                return;
            }

            // create bit-packed-header, which needs to fit in 1 byte
            byte bitPackHeader = (byte)((bitPackedGroupCount << 1) | 1);

            // update this byte
            baos[bitPackedRunHeaderPointer] = bitPackHeader;
            //baos.setByte(bitPackedRunHeaderPointer, bitPackHeader);

            // mark that this run is over
            bitPackedRunHeaderPointer = -1;

            // reset the number of groups
            bitPackedGroupCount = 0;
        }

        private void writeRleRun() {
            // we may have been working on a bit-packed-run
            // so close that run if it exists before writing this
            // rle-run
            endPreviousBitPackedRun();

            // write the rle-header (lsb of 0 signifies a rle run)
            //BytesUtils.writeUnsignedVarInt(repeatCount << 1, baos);
            Span<byte> buffer = stackalloc byte[10];
            int bytesWritten = 0;
            Parquet.Extensions.EncodingExtensions.ULEB128Encode((ulong)repeatCount << 1, buffer, ref bytesWritten);
            baos.AddRange(buffer.Slice(0, bytesWritten));

            // write the repeated-value
            //BytesUtils.writeIntLittleEndianPaddedOnBitWidth(baos, previousValue, bitWidth);
            switch(byteWidth) {
                case 1:
                    baos.Add((byte)previousValue);
                    break;
                case 2:
                    baos.Add((byte)previousValue);
                    baos.Add((byte)((previousValue >> 8) & 0xFF));
                    break;
                case 3:
                    baos.Add((byte)previousValue);
                    baos.Add((byte)((previousValue >> 8) & 0xFF));
                    baos.Add((byte)((previousValue >> 16) & 0x00FF));
                    break;
                case 4:
                    baos.Add((byte)previousValue);
                    baos.Add((byte)((previousValue >> 8) & 0xFF));
                    baos.Add((byte)((previousValue >> 16) & 0x00FF));
                    baos.Add((byte)((previousValue >> 32) & 0x0000FF));
                    break;
            }



            // reset the repeat count
            repeatCount = 0;

            // throw away all the buffered values, they were just repeats and they've been written
            numBufferedValues = 0;
        }

        public byte[] toBytes() {
            if(toBytesCalled)
                throw new Exception("You cannot call toBytes() more than once without calling reset()");

            // write anything that is buffered / queued up for an rle-run
            if(repeatCount >= 8) {
                writeRleRun();
            } else if(numBufferedValues > 0) {
                for(int i = numBufferedValues; i < 8; i++) {
                    bufferedValues[i] = 0;
                }
                writeOrAppendBitPackedRun();
                endPreviousBitPackedRun();
            } else {
                endPreviousBitPackedRun();
            }

            toBytesCalled = true;
            return baos.ToArray();
        }

        /**
         * Reset this encoder for re-use
         */
        public void reset() {
            reset(true);
        }


        public void close() {
            reset(false);
            //baos.close();
        }

        //public long getBufferedSize()
        //{
        //    return baos.size();
        //}

        //public long getAllocatedSize()
        //{
        //    //return baos.getCapacity();
        //}
    }
}