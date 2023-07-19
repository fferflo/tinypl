import tinypl as pl
import random
import numpy as np

class sampler:
    @staticmethod
    def sequential(dataset):
        while True:
            yield range(len(dataset))

    @staticmethod
    def shuffle(dataset, rng=random):
        while True:
            indices = list(range(len(dataset)))
            rng.shuffle(indices)
            yield indices

def load(dataset, batchsize, collate_fn=lambda batch: batch, map=pl.map, collate_map=None, sampler=None, epoch_end_mode="ignore"):
    if sampler is None:
        sampler = globals()["sampler"].sequential(dataset)
    if not epoch_end_mode in ["ignore", "keep_last", "drop_last"]:
        raise ValueError("epoch_end_mode must be one of [ignore, keep_last, drop_last]")
    split_on_marker = epoch_end_mode in ["keep_last", "drop_last"]

    pipe = sampler
    if epoch_end_mode == "drop_last":
        def cut_epoch(indices):
            indices = list(indices)
            n = len(indices) // batchsize * batchsize
            indices = indices[:n]
            return indices
        pipe = pl.map(pipe, cut_epoch)
    pipe, marker = pl.marker.insert_periodic(pipe, n=1, after=True) # End-of-epoch marker
    pipe = pl.flatten(pipe)

    if collate_map is None:
        # Yield one batch per worker
        pipe = pl.partition(pipe, batchsize, markers="after", split_on_marker=split_on_marker)

        pipe = order = pl.order.save(pipe, maxsize=1)
        pipe = pl.map(pipe, np.asarray)
        def load(indices):
            samples = [dataset[index] for index in indices]
            batch = collate_fn(samples)
            return batch
        pipe = map(pipe, load)
        if "maxsize" in dir(pipe):
            order.maxsize = pipe.maxsize
        pipe = pl.order.load(pipe, order)
    else:
        # Yield one item per worker, collate afterwards
        pipe = order = pl.order.save(pipe, maxsize=1)
        pipe = map(pipe, dataset.__getitem__)
        if "maxsize" in dir(pipe):
            order.maxsize = pipe.maxsize
        pipe = pl.order.load(pipe, order)

        pipe = pl.partition(pipe, batchsize, markers="after", split_on_marker=split_on_marker)
        pipe = order = pl.order.save(pipe, maxsize=1)
        pipe = collate_map(pipe, collate_fn)
        if "maxsize" in dir(pipe):
            order.maxsize = pipe.maxsize
        pipe = pl.order.load(pipe, order)

    return pipe