from PIL import Image, ImageOps
import numpy as np
import pandas as pd
import pickle
import os
from prefect import task, Flow

@task
def fuse_into_rgbd(artifact=None, base_data_path=None, **kwargs):
    print(kwargs)
    print(artifact)
    print(base_data_path)

    qrcode = artifact.split('_')[1]
    depth_path = base_data_path + '/depth/omdena_datasets/depthmap_training_dataset/scans/' + qrcode + '/100'
    rgb_path = base_data_path + '/rgb/omdena_datasets/rgb_training/scans/' + qrcode + '/100'
    output_path = base_data_path + '/rgbd/' + qrcode + '/100'

    with open(depth_path + '/' + artifact[:-2], 'rb') as f:
        depthmap, label = pickle.load(f)
    depthmap = depthmap.squeeze()

    metadata = pd.read_csv(base_data_path + '/pcd-rgb-metadata.csv')
    closest_rgb = metadata[metadata.artifact == artifact[:-2]].closest_rgb.values[0]

    im = Image.open(rgb_path + '/' + closest_rgb)
    im = im.resize(depthmap.shape)
    im = im.rotate(-90, expand=True)
    im = ImageOps.mirror(im)

    rgb = np.array(im)
    rgbd = np.concatenate([rgb, depthmap[..., np.newaxis]], axis=2)

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    with open(output_path + '/' + artifact[:-4] + '.rgbd', 'wb') as f:
        pickle.dump(rgbd, f)