# WISION-Lab Datasets

Our datasets are hosted on a publicly accessible S3 bucket. You can use the [aws-cli](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) to download individual objects or the whole dataset.

You can list all datasets and parts thereof like so:
```
aws s3 ls --summarize --human-readable --recursive s3://public-datasets/ --endpoint=https://web.s3.wisc.edu --no-sign-request
```

To only list data associated with a single dataset replace the URI above with one that matches the dataset prefix, e.g by using `s3://public-datasets/quanta-vision/sequences`.  

To download a specific object (where object-key is eg quanta-vision/sequences/README.md) you can use the following command:
```
aws s3api get-object --bucket public-datasets --key <OBJECT-KEY> --endpoint=https://web.s3.wisc.edu --no-sign-request <DOWNLOAD-PATH>
```

Finally, here's an example script which will download and unzip the whole `quanta-vision/sequences` dataset (warning ~2.4TB). You can use the same script with a different `DATASET_PREFIX` to download other datasets or subparts thereof:
```
#!/usr/bin/env bash

# Directory to download data to
DOWNLOAD_DIR=downloads/
DATASET_PREFIX=quanta-vision/sequences

# Clone all data from S3
aws s3 sync s3://public-datasets/$DATASET_PREFIX $DOWNLOAD_DIR --endpoint=https://web.s3.wisc.edu --no-sign-request

# Extract all zips in their CWD
for zip in $(find $DOWNLOAD_DIR -type f -name *.zip); do unzip $zip -d $(dirname $zip) && rm -f $zip; done
```

*Note:* If you are on UW-Madison wifi, downloads will be much faster.


## Quanta Vision Sequences

Below we include folder-wise descriptions (of directories under `sequences`), paper(s) associated with the folder and hot-pixel masks per sequence. These real-world sequences were captured using the passive single photon cameras, high speed cameras, or other specialized cameras (event/low light cameras).

* `anycam`: sequences associated with [Sundar et al., ICCV 2023](https://openaccess.thecvf.com/content/ICCV2023/html/Sundar_SoDaCam_Software-defined_Cameras_via_Single-Photon_Imaging_ICCV_2023_paper.html).  All sequences were captured at 96.8 kHz. Associated hot-pixel mask is `hot_pixel_mask/SwissSPAD_ddr3_mode.npy` for sequences captured by the SPAD with no color filter array and `hot_pixel_mask/colorSPAD_continuous_stream.npy` for the rest.  See `arguments.json` in each folder that contains a `binary.npy` file for discerning which is which. Alternatively, the mean-frame video gives it away (ones that use a CFA have a conspicuous mosaic pattern). When using the color-filter array captured sequences, we impute out the pixels corresponding to "R", "G", and "B" filters; these are a minority and make up just 6.25% of the overall pixel count.
* `color`: sequences associated with [Ma et al., SIGGRAPH 2023](https://dl.acm.org/doi/10.1145/3592438). Sequences were captured at 16 kHz (unless annotated otherwise) and use the hotpixel mask in `hot_pixel_mask/colorSPAD_continuous_stream.npy`. See `color_filter_array/rgbw_oh_bn_color_ss2_padded.tif` for a specification of the random RGBW CFA pattern.
* `pano`: sequences associated with [Jungerman et al., ICCV 2023](https://arxiv.org/abs/2309.03811). Sequences were captured at 96.8 kHz. Hot-pixel mask specified by `hot_pixel_mask/colorSPAD_continuous_stream.npy`. We use the color-filter array pattern with 93.75 % white or clear pixels and impute out the photon-cube locations associated with a "R", "G", or "B" photon detection.
* `photoev`: sequences associated with [Sundar et al., CVPR 2024](https://arxiv.org/abs/2407.02683). All sequences were captured at 96.8 kHz. Hot pixel masks are `hot_pixel_mask/new_graySPAD_continuous_stream.npy` for sequences captured by the SPAD with no CFAs and `hot_pixel_mask/colorSPAD_continuous_stream.npy` otherwise.
* `qbp`: sequences associated with [Ma et al., SIGGRAPH 2020](https://arxiv.org/abs/2006.11840). Hotpixel mask for all sequences is `hot_pixel_mask/SwissSPAD_ddr3_mode.npy`. Sequences captured at 10--16 kHz.
* `vision`: sequences associated with [Ma et al., WACV 2023](https://openaccess.thecvf.com/content/WACV2023/papers/Ma_Burst_Vision_Using_Single-Photon_Cameras_WACV_2023_paper.pdf). Hotpixel mask for all sequences is `hot_pixel_mask/SwissSPAD_continuous_stream.npy`. Most sequences were captured at 10--16 kHz.

<details>
<summary>See Detailed Folder Structure</summary>

```
ROOT: quanta_vision/sequences
├── (ZIP 29.9G) 📁 qbp                                                    
├── (ZIP 927.4K) 📁 masks
├── 📁 anycam
│   ├── (ZIP #1/3 98.8G) balloon_burst_17th_Dec_2022, bubble_machine_17th_Dec_2022, capitol_24th_Feb_2023, casino_roulette_10th_Feb_2023, confetti_popper_17th_Dec_2022, eye_track_17th_Dec_2022, falling_dice_9th_Dec_2022, falling_dice_11th_Dec_2022, jack-in-the-box_17th_Dec_2022, measuring_tape_17th_Dec_2022
│   ├── (ZIP #2/3 94.3G) newton_cradle_8th_Feb_2023, party_popper_17th_Dec_2022, pedestrian_24th_Feb_2023, ramanujam_24th_Feb_2023, sanity, tabletop_24th_Feb_2023, traffic_10th_Feb_2023, vanvleck_24th_Feb_2023
│   └── (ZIP #3/3 101.4G) vertical_wheel_10th_Feb_2023, vertical_wheel_17th_Dec_2022, vertical_wheel_colorSPAD_10th_Feb_2023, water_meniscus_17th_Dec_2022, falling_dice.mp4
├── 📁 color
│   ├── (ZIP #1/6 90.4G) 1221_May_8th, 1240_May_8th, 1240_backstage_May_8th, 1240_gray_panel_May_8th, 1325_May_8th, HDR_April_27th, HDR_white_vase_10th_November, HDR_white_vase_19th_October, LED_balloon_May_3rd
│   ├── (ZIP #2/6 108.6G) LED_balloons_Mat_13th, all_dark, all_dark_25th_September, all_white, balloon_burst_April_27th, bouncy_balls_July_12th, bouncy_balls_July_27th, bubbles_April_28th, casino_roulette_July_12th
│   ├── (ZIP #3/6 99.7G) casino_roulette_July_27th, chair_May_28th, cloth_April_26th, color_chart_April_26th, colored_dice_July_27th, dartboard_May_25th, darts_April_26th, dice_July_8th, dry_run, dry_run_April_14th, dry_run_April_14th_8pm, entrance_HDR_July_27th, entrance_May_8th
│   ├── (ZIP #4/6 84.5G) entrance_May_16th, entrance_May_27th, entrance_May_30th, feathers_April_27th, feathers_May_30th, fence_structure_June_8th, fence_structure_May_30th
│   ├── (ZIP #5/6 94.6G) front_entrance_May_16th, fruits_May_30th, grafitti_elephant_20th_October, hdr_entrance_July_21st, hdr_entrance_table_July_21st, jack-in-the-box_June_8th, jack-in-the-box_May_30th, lighter_April_21st, lighter_May_3rd, potted_plant_1309_May_27th, potted_plant_June_8th
│   └── (ZIP #6/6 93.5G) tabletop_April_20th, toy_fence_June_1st, vase_HDR_5th_April_2023, vase_HDR_Sept_13th, vertical_wheel_July_11th, vertical_wheel_July_15th, waveform_LED_17th_April_2023, waving_cloth_May_30th, rgbw_oh_bn_color_ss2_padded.tif
├── 📁 pano
│   ├── (ZIP #1/2 82.4G) cs6floorlounge, vanvleck
│   └── (ZIP #2/2 92.0G) vanvleck2
├── 📁 photoev
│   ├── (ZIP #1/2 97.4G) blender_1st_Sept, blender_almonds_1st_Sept, blender_almonds_take_2_1st_Sept, blender_almonds_take_3_1st_Sept, darts_22nd_Sept, darts_26th_Sept_ambient, darts_26th_Sept_dark, darts_26th_Sept_dark_1lux, darts_26th_Sept_dark_2lux_2023-09-26--16-56-48, darts_26th_Sept_dark_5lux, darts_low_light_2_22nd_Sept, darts_low_light_3_22nd_Sept, darts_low_light_4_22nd_Sept, darts_low_light_22nd_Sept, drill_1st_Sept, drill_take_2_1st_Sept, drill_take_3_1st_Sept, dslr_shutter, flag_6th_floor_13th_Sept, flag_6th_floor_13th_Sept_take2, iphone_lock_screen, iphone_lock_screen_20_per, iphone_lock_screen_80_per, iphone_screen_20_per, iphone_screen_20_per_2023-11-10--19-00-49, iphone_screen_80_per, leaf_blower_1st_Sept, lighter_1st_Sept, lighter_take_2_1st_Sept
│   └── (ZIP #2/2 92.3G) lighter_take_3_1st_Sept, phone, phone_screen, prophesee, slingshot_1st_Sept, slingshot_13th_Nov_2023-11-13--14-32-28, slingshot_13th_Nov_2023-11-13--14-38-03, slingshot_13th_Nov_2023-11-13--14-41-33, slingshot_13th_Nov_2023-11-13--14-44-55, slingshot_13th_Nov_prophesee, stress_ball_1st_Sept, stress_ball_take_2_1st_Sept, stressball_29th_Sept, stressball_29th_Sept_12mm_prophesee, stressball_29th_Sept_16mm_infinicam, stressball_29th_Sept_2023-09-29--15-11-44, tennis_27th_Sept_75mm_2023-09-27--17-12-32, tennis_27th_Sept_75mm_2023-09-27--17-13-13, tennis_27th_Sept_75mm_2023-09-27--17-14-18, tennis_50mm_27th_Sept_rear_2023-09-27--17-58-43, tennis_50mm_27th_Sept_rear_2023-09-27--17-59-23, tennis_50mm_27th_Sept_rear_2023-09-27--17-59-58, tennis_50mm_27th_Sept_rear_2023-09-27--18-00-44, tennis_100mm_27th_Sept_2023-09-27--17-37-52, tennis_100mm_27th_Sept_2023-09-27--17-39-05, tennis_prophesee, traffic_8pm_27th_Sept_2023-09-27--20-04-33, traffic_8pm_27th_Sept_2023-09-27--20-11-42, traffic_8pm_27th_Sept_2023-09-27--20-14-31, traffic_8pm_27th_Sept_2023-09-27--20-17-33, traffic_8pm_27th_Sept_prophesee
├── 📁 vision
│   ├── (ZIP #1/12 97.7G) 0505-bicycle-1, 0505-bicycle-2, 0505-bicycle-3, 0505-bicycle-4, 0505-bicycle-5, 0505-face-1, 0505-face-2, 0505-face-3, 0505-face-4, 0525-newton-1, 0525-newton-2, 0525-newton-3, 0525-newton-4, 0525-newton-5, 0527-train-bright, 0527-train-switch, 0528-pendulum-1, 0528-pendulum-2, 0528-pendulum-3, 0528-train-dark-1, 0528-train-dark-2, 0528-train-switch-1, 0528-train-switch-2, 0531-spinner-1, 0531-spinner-2
│   ├── (ZIP #2/12 98.4G) 0531-spinner-3, 0602-street, 0604-actions-1, 0604-actions-2, 0604-actions-3, 0604-ball-1, 0604-ball-2, 0604-ball-3, 0604-chair-0, 0604-chair-1, 0604-chair-2, 0604-chair-3, 0604-face-0, 0604-face-1, 0604-face-2, 0604-face-3, 0604-jump-1, 0604-jump-2, 0604-jump-3, 0604-runwalk-1, 0604-runwalk-2, 0604-runwalk-3, 0604-throwdrink-0, 0604-throwdrink-1, 0604-throwdrink-2, 0604-throwdrink-3, 0604-walk-1, 0604-walk-2, 0604-walk-3, 0604-walk-4, 0604-walk-5, 0608-street-1
│   ├── (ZIP #3/12 99.5G) 0608-street-2, 0608-street-3, 0609-handheld-1, 0609-handheld-2, 0609-handheld-3, 0614-calib-1, 0614-calib-2, 0614-calib-3, 0614-calib-4, 0614-calib-5, 0702-moving-bike-dark-1, 0702-moving-drive, 0702-moving-ocr-1, 0702-moving-ocr-2, 0702-moving-ocr-3, 0702-moving-walktoward-1, 0702-moving-walktoward-dark-1, 0702-moving-walktoward-dark-2, 0702-static-bike-1, 0702-static-bike-2, 0702-static-bike-dark-1, 0702-static-jump-ddark-1, 0702-static-run-1
│   ├── (ZIP #4/12 87.5G) 0702-static-run-2, 0702-static-run-dark-1, 0702-static-run-ddark-1, 0702-static-walk-1, 0702-static-walk-2, 0702-static-walk-dark-1, 0702-static-walk-ddark-1, 0702-static-walktoward-ddark-1, 0723-calib8mm-1, 0723-calib8mm-2, 0723-calib8mm-3, 0723-calib16mm-1, 0723-calib16mm-2, 0723-calib16mm-3, 0723-calib16mm-4
│   ├── (ZIP #5/12 97.7G) 0815-warf-1, 0815-warf-2, 0815-warf-bright
│   ├── (ZIP #6/12 111.4G) 0815-warf-long, 0815-warf-slow, 0905-ball-mohit-l0, 0905-ball-mohit-l1, 0905-ball-mohit-l2, 0905-ball-sizhuo-l0, 0905-ball-sizhuo-l2, 0905-hdr-sizhuo-f13d2, 0905-hdr-sizhuo-f16
│   ├── (ZIP #7/12 91.1G) 0905-hdr-sizhuo-f16-0, 0905-jump-mohit-l0, 0905-jump-mohit-l1, 0905-jump-mohit-l2, 0905-jump-sizhuo-l0, 0905-jump-sizhuo-l1, 0905-jump-sizhuo-l2, 0905-walk-mohit-l0, 0905-walk-mohit-l1, 0905-walk-mohit-l2, 0905-walk-sizhuo-l0, 0905-walk-sizhuo-l1, 0905-walk-sizhuo-l2, 1005-ocr-far-l1, 1005-ocr-far-l1-test, 1005-ocr-far-l2, 1005-ocr-far-strobe, 1005-ocr-far-strobe-2, 1005-ocr-far-strobe-3, 1005-ocr-near-l1, 1005-ocr-near-l1-2, 1005-ocr-near-l2, 1005-ocr-near-l2-2, 1005-ocr-near-strobe, 1005-ocr-near-strobe-2
│   ├── (ZIP #8/12 29.8G) 1007-bike-1, 1007-bike-2
│   ├── (ZIP #9/12 137.5G) 1007-drive-1
│   ├── (ZIP #10/12 139.0G) 1007-drive-2
│   ├── (ZIP #11/12 108.1G) 1007-walk-1, 1007-walk-2, 1007-walk-3, 1014-slam-l0, 1014-slam-l0-2, 1014-slam-l0-3, 1014-slam-l0-4, 1014-slam-l0-5, 1014-slam-l1
│   └── (ZIP #12/12 49.6G) 1014-slam-l2, 1014-slam-l2-2, 1014-slam-l3, 1014-slam-l4
└── 📄 README.md
```

*Note:* The zip file sizes refer to the decompressed filesize.

</details>


## VisionSIM-50 Dataset (pre-release) 

Using the [visionsim framework](https://github.com/WISION-Lab/visionsim) you can simulate large scale datasets with a wide range of ground truth annotations and realistic sensor emulations. Here we provide access to the [dataset which was created as part of this tutorial.](https://visionsim.readthedocs.io/en/latest/tutorials/large-dataset.html) It contains 50 indoor scenes with realistic camera motion which are animated for 12 seconds and rendered at `100fps` at a resolution of `800x800` pixels. Ground truth annotation for metric depths, normals, optical flow (both forward and backwards), object segmentations, as well as camera intrinsics and extrinsics are provided for every frame.

*Note:* This is a pre-release dataset and is subject to change or get updated. 

<details>
<summary>See Detailed Folder Structure</summary>

```
ROOT: visionsim/visionsim50-1seq-100fps
├── (ZIP 6.5G) 📁 previews
└── 📁 renders
    ├── (ZIP #1/10 78.7G) attic, bachelors-quarters, barbershop, bath
    ├── (ZIP #2/10 118.6G) bathroom1, bathroom2, bathroom3, bathroom4, bathroom5, bathtime
    ├── (ZIP #3/10 73.9G) bedroom1, bedroom2, classroom, cocina-ii
    ├── (ZIP #4/10 113.5G) country-kitchen, cozykitchen, designer-bedroom, diner, diningroom
    ├── (ZIP #5/10 101.0G) domestic-office-table, gaffer, game-room, interior-scene, italianflat
    ├── (ZIP #6/10 96.3G) junkshop, kitchen1, kitchen2, kitchen3, kitchenpack, lazienka
    ├── (ZIP #7/10 102.0G) library-homeoffice, livingroom, loft, lynxsdesign, mesa-concept, minimarket
    ├── (ZIP #8/10 115.8G) modern-kitchen, morning-apartment, officebuilding, paneled-room-revisited, restaurant, restroom
    ├── (ZIP #9/10 98.7G) simplekitchen, staircase, stone-shower, sunny-room, tv-couch, ultramodern
    └── (ZIP #10/10 44.4G) white-room, wooden-staircase
```

*Note:* The zip file sizes refer to the decompressed filesize.

</details>
