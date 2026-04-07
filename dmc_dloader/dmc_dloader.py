import os
import torch
import utils
from torchvision.datasets import ImageFolder
from torch.utils.data import ConcatDataset

class DMC_DLOADER:
    def __init__(self, device, debug=False):
        self.device = device
        self.debug = debug
        self.shard_root = "local_shards"

    def get_primary_shards(self):
        placement_map = utils.load_placement_map()
        my_shards = utils.get_my_shards(placement_map, utils.find_own_ip())

        if not my_shards:
            raise ValueError(
                f"No primary shards assigned to node with IP {utils.find_own_ip()}"
            )
        
        if self.debug:
            print(f"[DMC-DLoader] Node primary shards: {my_shards}")

        return my_shards

    def build_dataset(self, dataset_cls=None, transform=None):
        datasets = []
        primary_shards = self.get_primary_shards()

        for shard_id in primary_shards:
            shard_path = os.path.join(self.shard_root, f"shard_{shard_id}")

            if not os.path.exists(shard_path):
                raise FileNotFoundError(
                    f"Shard path {shard_path} does not exist for node {self.node_id}"
                )

            ds = dataset_cls(shard_path, transform=transform)
            datasets.append(ds)

        final_dataset = ConcatDataset(datasets)

        if self.debug:
            print(f"[DMC-DLoader] Total samples: {len(final_dataset)}")

        return final_dataset