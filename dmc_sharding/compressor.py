import gzip
import lz4.frame
import zstandard as zstd
import os

BUFFER_SIZE = 1024 * 1024  # 1MB


class Compressor:
    def compress(self, src: str, dst: str):
        raise NotImplementedError

    def decompress(self, src: str, dst: str):
        raise NotImplementedError


# ---------------- ZSTD ---------------- #

class ZstdCompressor(Compressor):

    def __init__(self, level: int = 3):
        self.level = level

    def compress(self, src: str, dst: str):

        cctx = zstd.ZstdCompressor(level=self.level)

        with open(src, "rb") as fin, open(dst, "wb") as fout:
            with cctx.stream_writer(fout) as compressor:
                while chunk := fin.read(BUFFER_SIZE):
                    compressor.write(chunk)

    def decompress(self, src: str, dst: str):

        dctx = zstd.ZstdDecompressor()

        with open(src, "rb") as fin, open(dst, "wb") as fout:
            with dctx.stream_reader(fin) as reader:
                while chunk := reader.read(BUFFER_SIZE):
                    fout.write(chunk)


# ---------------- LZ4 ---------------- #

class LZ4Compressor(Compressor):

    def compress(self, src: str, dst: str):

        with open(src, "rb") as fin, lz4.frame.open(dst, "wb") as fout:
            while chunk := fin.read(BUFFER_SIZE):
                fout.write(chunk)

    def decompress(self, src: str, dst: str):

        with lz4.frame.open(src, "rb") as fin, open(dst, "wb") as fout:
            while chunk := fin.read(BUFFER_SIZE):
                fout.write(chunk)


# ---------------- GZIP ---------------- #

class GzipCompressor(Compressor):

    def compress(self, src: str, dst: str):

        with open(src, "rb") as fin, gzip.open(dst, "wb") as fout:
            while chunk := fin.read(BUFFER_SIZE):
                fout.write(chunk)

    def decompress(self, src: str, dst: str):

        with gzip.open(src, "rb") as fin, open(dst, "wb") as fout:
            while chunk := fin.read(BUFFER_SIZE):
                fout.write(chunk)


# ---------------- Factory ---------------- #

def get_compressor(name: str) -> Compressor:

    name = name.lower()

    if name == "zstd":
        return ZstdCompressor()

    if name == "lz4":
        return LZ4Compressor()

    if name == "gzip":
        return GzipCompressor()

    raise ValueError(f"Unsupported compression: {name}")