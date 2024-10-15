
from contextlib import closing
from io import BytesIO
import sqlite3
from typing import Any, Generator, Sequence
from hscifsspecutil import get_async_filesystem, cache_locally_if_remote, prefetch_if_remote, PLocalAFetcher, fetch_and_transform_async
from multiprocessing_utils import SharedLock
from stream_unzip import stream_unzip

def _uncompress(compressed_bytes: bytes) -> BytesIO:
    _, _, biter = next(stream_unzip([compressed_bytes]))
    bio = BytesIO()
    for b in biter:
        bio.write(b)
    return bio

class S3ZipDataAccess():

    _lock = SharedLock()

    def __init__(self, 
                 sqlite_url: str,
                 table_name: str,
                 index_column: str, 
                 id_column: str, 
                 offset_column: str,
                 length_column: str, 
                 zip_url: str,
                 cache_dir: str, 
                 disable_caching: bool = False,
                 disable_prefetching: bool = True,
                 storage_options: dict = {}):
        zip_size = get_async_filesystem(zip_url, storage_options).size(zip_url)
        self.afetcher = PLocalAFetcher(zip_url, zip_size, storage_options=storage_options, cache_dir=cache_dir if not disable_caching else None)
        with S3ZipDataAccess._lock:
            self.sqlite = sqlite3.connect(cache_locally_if_remote(sqlite_url, storage_options=storage_options, cache_dir=cache_dir))
            if not disable_caching and not disable_prefetching:
                prefetch_if_remote(zip_url, zip_size, cache_dir, storage_options)
        self.table_name = table_name
        self.index_column = index_column
        self.id_column = id_column
        self.id_offset_len_columns = f"{id_column}, {offset_column}, {length_column}"
        self._len = None

    def __len__(self):
        if self._len is None:
            with closing(self.sqlite.execute(
                    f"SELECT COUNT(*) FROM {self.table_name}")) as cur:
                self._len = cur.fetchall()[0][0]
        return self._len

    def keys(self) -> Generator[str, None, None]:
        with closing(self.sqlite.execute(f"SELECT {self.id_column} FROM {self.table_name}")) as cur:
            for id, in cur:
                yield id
            
    def __getitems__(self, idxs: Sequence[int | str]) -> Sequence[BytesIO]:
        if isinstance(idxs[0], int):
            ids_offsets_lens = self.sqlite.execute(f"SELECT {self.id_offset_len_columns} FROM {self.table_name} WHERE {self.index_column} IN (%s)" % ','.join(
                '?' * len(idxs)), idxs).fetchall()
        else:
            ids_offsets_lens = self.sqlite.execute(f"SELECT {self.id_offset_len_columns} FROM {self.table_name} WHERE {self.id_column} IN (%s)" % ','.join(
                '?' * len(idxs)), idxs).fetchall()
        ido = {id: idx for idx, (id, _, _) in enumerate(ids_offsets_lens)}
        bytess = fetch_and_transform_async(self.afetcher, [(offset, offset + length) for _, offset, length in ids_offsets_lens], _uncompress)
        return [bytess[ido[id]] for id in idxs]


    def __getitem__(self, key: str) -> BytesIO:
        return self.__getitems__([key])[0]

        