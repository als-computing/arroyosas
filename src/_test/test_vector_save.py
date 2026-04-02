"""Tests for arroyosas.lse_reduction.vector_save"""

import pytest

from arroyosas.lse_reduction.schemas import LatentSpaceEvent
from arroyosas.lse_reduction.vector_save import VectorSavePublisher
from arroyosas.schemas import SASStart, SASStop

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def publisher(tmp_path):
    db_path = str(tmp_path / "test_vectors.db")
    pub = VectorSavePublisher(db_path=db_path)
    await pub.start()
    yield pub
    if pub.db:
        await pub.db.close()


class TestVectorSavePublisher:
    async def test_start_initializes_db(self, tmp_path):
        db_path = str(tmp_path / "init_test.db")
        pub = VectorSavePublisher(db_path=db_path)
        assert not pub._db_initialized
        await pub.start()
        assert pub._db_initialized
        assert pub.db is not None
        await pub.db.close()

    async def test_init_db_idempotent(self, publisher):
        # Calling _init_db twice should not fail
        await publisher._init_db()
        assert publisher._db_initialized

    async def test_save_vector_basic(self, publisher):
        await publisher.save_vector(
            tiled_url="http://example.com/tiled",
            feature_vector=[1.0, 2.0, 3.0],
            autoencoder_model="ae_v1",
            dimred_model="umap_v1",
        )
        async with publisher.db.execute("SELECT * FROM vectors") as cursor:
            rows = await cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][1] == "http://example.com/tiled"  # tiled_url column

    async def test_save_vector_with_all_fields(self, publisher):
        await publisher.save_vector(
            tiled_url="http://example.com/tiled",
            feature_vector=[0.1, 0.2],
            autoencoder_model="ae",
            dimred_model="umap",
            experiment_name="my_exp",
            timestamp=1000.0,
            total_processing_time=0.5,
            autoencoder_time=0.3,
            dimred_time=0.2,
        )
        async with publisher.db.execute("SELECT * FROM vectors") as cursor:
            row = await cursor.fetchone()
        assert row[5] == "my_exp"  # experiment_name
        assert row[6] == 1000.0  # timestamp

    async def test_publish_with_latent_space_event(self, publisher):
        event = LatentSpaceEvent(
            tiled_url="http://example.com/run/uuid-1234",
            feature_vector=[0.1, 0.2, 0.3],
            index=0,
            autoencoder_model="ae_v1",
            dimred_model="umap_v1",
            experiment_name="exp_1",
            timestamp=999.0,
            total_processing_time=0.1,
            autoencoder_time=0.05,
            dimred_time=0.05,
        )
        await publisher.publish(event)
        async with publisher.db.execute("SELECT COUNT(*) FROM vectors") as cursor:
            count = await cursor.fetchone()
        assert count[0] == 1

    async def test_publish_ignores_non_latent_space_event(self, publisher):
        # SASStart is not a LatentSpaceEvent
        start = SASStart(
            run_name="test_run",
            run_id="123",
            width=10,
            height=10,
            data_type="float32",
            tiled_url="http://example.com",
        )
        result = await publisher.publish(start)
        assert result is None
        async with publisher.db.execute("SELECT COUNT(*) FROM vectors") as cursor:
            count = await cursor.fetchone()
        assert count[0] == 0

    async def test_publish_ignores_sas_stop(self, publisher):
        stop = SASStop(num_frames=5)
        result = await publisher.publish(stop)
        assert result is None

    async def test_multiple_saves(self, publisher):
        for i in range(5):
            await publisher.save_vector(
                tiled_url=f"http://example.com/{i}",
                feature_vector=[float(i)],
                autoencoder_model="ae",
                dimred_model="umap",
            )
        async with publisher.db.execute("SELECT COUNT(*) FROM vectors") as cursor:
            count = await cursor.fetchone()
        assert count[0] == 5

    async def test_lazy_init_on_save(self, tmp_path):
        db_path = str(tmp_path / "lazy.db")
        pub = VectorSavePublisher(db_path=db_path)
        assert not pub._db_initialized
        # Calling save_vector should auto-initialize
        await pub.save_vector("url", [1.0], "ae", "umap")
        assert pub._db_initialized
        await pub.db.close()
