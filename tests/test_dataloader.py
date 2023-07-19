import tinypl as pl
import pytest, time, random
from functools import partial
import numpy as np

class Dataset:
    def __init__(self, n):
        self.n = n

    def __len__(self):
        return self.n

    def __getitem__(self, idx):
        time.sleep(random.uniform(0.01, 0.001))
        return idx

maps = [partial(pl.thread.map, workers=4), partial(pl.process.map, workers=4), pl.map]

@pytest.mark.parametrize("map", maps)
@pytest.mark.parametrize("collate_map", maps + [None])
def test_map_functions(map, collate_map):
    dataset = Dataset(10)

    pipe = pl.dataloader.load(
        dataset,
        batchsize=4,
        sampler=pl.dataloader.sampler.sequential(dataset),
        map=map,
        collate_fn=np.asarray,
        collate_map=collate_map,
        epoch_end_mode="ignore",
    )

    for i in range(10):
        assert next(pipe).tolist() == [(i * 4 + o) % len(dataset) for o in range(4)]

def test_drop_last():
    dataset = Dataset(10)

    pipe = pl.dataloader.load(
        dataset,
        batchsize=4,
        sampler=pl.dataloader.sampler.sequential(dataset),
        map=partial(pl.thread.map, workers=4),
        collate_fn=np.asarray,
        epoch_end_mode="drop_last",
    )

    for i in range(10):
        if i % 2 == 0:
            assert next(pipe).tolist() == [0, 1, 2, 3]
        elif i % 2 == 1:
            assert next(pipe).tolist() == [4, 5, 6, 7]

def test_keep_last():
    dataset = Dataset(10)

    pipe = pl.dataloader.load(
        dataset,
        batchsize=4,
        sampler=pl.dataloader.sampler.sequential(dataset),
        map=partial(pl.thread.map, workers=4),
        collate_fn=np.asarray,
        epoch_end_mode="keep_last",
    )

    for i in range(10):
        if i % 3 == 0:
            assert next(pipe).tolist() == [0, 1, 2, 3]
        elif i % 3 == 1:
            assert next(pipe).tolist() == [4, 5, 6, 7]
        elif i % 3 == 2:
            assert next(pipe).tolist() == [8, 9]

def test_shuffle():
    dataset = Dataset(10)

    pipe = pl.dataloader.load(
        dataset,
        batchsize=4,
        sampler=pl.dataloader.sampler.shuffle(dataset),
        map=partial(pl.thread.map, workers=4),
        collate_fn=np.asarray,
        collate_map=partial(pl.thread.map, workers=4),
        epoch_end_mode="ignore",
    )

    for i in range(10):
        assert len(next(pipe).tolist()) == 4