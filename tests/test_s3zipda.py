import itertools
import s3zipda
from hscifsspecutil import get_s3fs_credentials
from hereutil import here
from PIL import Image

def test_s3zipda():
    da = s3zipda.S3ZipDataAccess(
        table_name="ecco_pages",
        index_column="entry_number",
        id_column="id",
        offset_column="img_offset",
        length_column="img_compressed_size + 128",
        sqlite_url = "s3://ecco-data/ecco_good_page_image_and_text_offsets.sqlite3",
        zip_url = "s3://ecco-data/ecco_page_images.zip",
        storage_options=get_s3fs_credentials(here("s3_secret.yaml")),
        cache_dir=str(here("cache")),
        disable_caching=True
    )
    assert len(da) == 32335047
    first_items = ['000010010000010', '000010010000020', '000010010000030', '000010010000040']
    assert list(itertools.islice(da.keys(), 0, 4)) == first_items
    im = Image.open(da['000010010000010'])
    assert im.size == (992, 2048)
    assert [Image.open(b).size for b in  da.__getitems__(['000010010000010'])] == [(992, 2048)]
    assert [Image.open(b).size for b in  da.__getitems__(first_items)] == [(992, 2048), (896, 2016), (992, 2080), (1088, 2112)]
