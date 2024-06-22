import tinypl as pl
import random
import numpy as np

class sampler:
    @staticmethod
    def sequential(dataset, epochs=None):
        e = 0
        while True:
            yield range(len(dataset))
            e += 1
            if not epochs is None and e == epochs:
                break

    @staticmethod
    def shuffle(dataset, rng=random, epochs=None):
        e = 0
        while True:
            indices = list(range(len(dataset)))
            rng.shuffle(indices)
            yield indices
            e += 1
            if not epochs is None and e == epochs:
                break

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
        if map == pl.map:
            # Yield items and collate per worker
            pipe = pl.partition(pipe, batchsize, markers="after", split_on_marker=split_on_marker)

            pipe = order = pl.order.save(pipe, maxsize=1)
            pipe = pl.map(pipe, np.asarray)
            def load(indices):
                return [dataset[index] for index in indices]
            pipe = pl.map(pipe, load)
            pipe = collate_map(pipe, collate_fn)
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