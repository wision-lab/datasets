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
for zip in $(find $DOWNLOAD_DIR -type f -name *.zip); do 7z x $zip -o$(dirname $zip) && rm -f $zip; done
```

*Note:* Some archives use LZMA for higher compression ratios, you can use the `7z` cli to unzip these (as above), but the `unzip` command might not work.  
*Note:* If you are on UW-Madison wifi or connected to the campus VPN, downloads will be much faster.



# Captured Datasets

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
</br>

# Simulated Datasets

## Single Photon Reconstruction Challenge

This dataset was created for an ongoing [single photon reconstruction challenge and competition](https://singlephotonchallenge.com/) and consists of
photoncube/image pairs from 50 unique simulated scenes, plus another 5 scenes for the test set (for which ground
truths are not made public). Each photoncube consists of 1024 bitplanes, and the associated ground truth
reconstruction corresponds to the last bitplane. A sample of this dataset can be [downloaded here](https://drive.google.com/file/d/1wV5KnbexqOXVS69SfawPBZu0AVk-Tu_v/view?usp=sharing) (~3.5GB).

To download the whole dataset (~425G training set + ~42G test set, ~133G and ~13G compressed respectively) use `DATASET_PREFIX=challenges/reconstruction`.

<details>
<summary>See Detailed Folder Structure</summary>

<pre>
Tree<'train.json'>
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_0.zip">train_0.zip</a> (8.5G 14.0x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_1.zip">train_1.zip</a> (8.5G 4.1x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_2.zip">train_2.zip</a> (8.5G 3.0x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_3.zip">train_3.zip</a> (8.5G 4.1x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_4.zip">train_4.zip</a> (8.5G 2.2x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_5.zip">train_5.zip</a> (8.5G 22.1x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_6.zip">train_6.zip</a> (8.5G 4.3x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_7.zip">train_7.zip</a> (8.5G 5.3x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_8.zip">train_8.zip</a> (8.5G 2.1x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_9.zip">train_9.zip</a> (8.5G 2.3x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_10.zip">train_10.zip</a> (8.5G 2.9x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_11.zip">train_11.zip</a> (8.5G 6.1x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_12.zip">train_12.zip</a> (8.5G 3.5x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_13.zip">train_13.zip</a> (8.5G 3.6x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_14.zip">train_14.zip</a> (8.5G 3.1x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_15.zip">train_15.zip</a> (8.5G 2.3x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_16.zip">train_16.zip</a> (8.5G 2.8x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_17.zip">train_17.zip</a> (8.5G 5.3x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_18.zip">train_18.zip</a> (8.5G 2.7x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_19.zip">train_19.zip</a> (8.5G 2.6x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_20.zip">train_20.zip</a> (8.5G 3.4x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_21.zip">train_21.zip</a> (8.5G 4.4x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_22.zip">train_22.zip</a> (8.5G 2.9x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_23.zip">train_23.zip</a> (8.5G 3.2x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_24.zip">train_24.zip</a> (8.5G 5.4x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_25.zip">train_25.zip</a> (8.5G 3.4x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_26.zip">train_26.zip</a> (8.5G 2.2x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_27.zip">train_27.zip</a> (8.5G 4.2x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_28.zip">train_28.zip</a> (8.5G 2.6x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_29.zip">train_29.zip</a> (8.5G 2.2x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_30.zip">train_30.zip</a> (8.5G 3.4x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_31.zip">train_31.zip</a> (8.5G 2.9x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_32.zip">train_32.zip</a> (8.5G 2.2x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_33.zip">train_33.zip</a> (8.5G 2.4x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_34.zip">train_34.zip</a> (8.5G 3.3x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_35.zip">train_35.zip</a> (8.5G 2.6x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_36.zip">train_36.zip</a> (8.5G 1.7x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_37.zip">train_37.zip</a> (8.5G 3.9x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_38.zip">train_38.zip</a> (8.5G 3.9x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_39.zip">train_39.zip</a> (8.5G 3.7x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_40.zip">train_40.zip</a> (8.5G 2.0x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_41.zip">train_41.zip</a> (8.5G 5.1x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_42.zip">train_42.zip</a> (8.5G 2.3x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_43.zip">train_43.zip</a> (8.5G 5.2x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_44.zip">train_44.zip</a> (8.5G 7.1x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_45.zip">train_45.zip</a> (8.5G 2.3x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_46.zip">train_46.zip</a> (8.5G 3.8x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_47.zip">train_47.zip</a> (8.5G 2.5x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_48.zip">train_48.zip</a> (8.5G 3.3x)
&#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/train_49.zip">train_49.zip</a> (8.5G 6.5x)
</pre>

<pre>
Tree<'test.json'>
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/test_0.zip">test_0.zip</a> (8.5G 8.8x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/test_1.zip">test_1.zip</a> (8.5G 1.4x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/test_2.zip">test_2.zip</a> (8.5G 9.7x)
&#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/test_3.zip">test_3.zip</a> (8.5G 2.3x)
&#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/challenges/reconstruction/test_4.zip">test_4.zip</a> (8.5G 6.9x)
</pre>

*Note:* The zip file sizes refer to the decompressed filesize, compression ratio is shown in parenthesis.

*Tip:* To see the full details for each split, you can use the `show-tree` like so `uv run tools.py show-tree trees/challenges/reconstruction/test.json --full`. To generate the tree with direct download links, use `uv run tools.py show-tree trees/challenges/reconstruction/test.json --s3-prefix challenges/reconstruction`. Use `--html` to render links as HTML `<a>` tags (needed inside `<details>` tags).

</details>


## VisionSIM-50 Dataset (pre-release) 

Using the [visionsim framework](https://github.com/WISION-Lab/visionsim) you can simulate large scale datasets with a wide range of ground truth annotations and realistic sensor emulations. Here we provide access to the [dataset which was created as part of this tutorial.](https://visionsim.readthedocs.io/en/latest/tutorials/large-dataset.html) It contains 50 indoor scenes with realistic camera motion which are animated for 12 seconds and rendered at `100fps` at a resolution of `800x800` pixels. Ground truth annotation for metric depths, normals, optical flow (both forward and backwards), object segmentations, as well as camera intrinsics and extrinsics are provided for every frame.

This dataset has been split by types of ground truth annotations before being uploaded, so for instance, you'll find all the depth maps under `visionsim/visionsim50/depths`, RGB frames under `visionsim/visionsim50/frames`, etc. The previews folder contains video previews of all scenes and ground truths, and the metadata folder has all the `transforms.json` files which contain camera intrinsics and extrinsics. 

For instance, to download only the RGB data and camera trajectories, you can run the above script with `DATASET_PREFIX=visionsim/visionsim50/frames` and again with `DATASET_PREFIX=visionsim/visionsim50/metadata`.

*Note:* This is a pre-release dataset and is subject to change or get updated. 

<details>
<summary>See Detailed Folder Structure</summary>

<pre>
Tree<'frames.json'>
&#x2570;&#x2500;&#x2500; 📁 datasets (44.5G)
    &#x2570;&#x2500;&#x2500; 📁 renders (44.5G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/attic_0.zip">attic_0.zip</a> (768.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/bachelors-quarters_0.zip">bachelors-quarters_0.zip</a> (885.9M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/barbershop_0.zip">barbershop_0.zip</a> (930.2M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/bath_0.zip">bath_0.zip</a> (659.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/bathroom1_0.zip">bathroom1_0.zip</a> (779.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/bathroom2_0.zip">bathroom2_0.zip</a> (452.2M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/bathroom3_0.zip">bathroom3_0.zip</a> (1.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/bathroom4_0.zip">bathroom4_0.zip</a> (950.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/bathroom5_0.zip">bathroom5_0.zip</a> (833.3M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/bathtime_0.zip">bathtime_0.zip</a> (928.3M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/bedroom1_0.zip">bedroom1_0.zip</a> (638.5M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/bedroom2_0.zip">bedroom2_0.zip</a> (840.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/classroom_0.zip">classroom_0.zip</a> (878.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/cocina-ii_0.zip">cocina-ii_0.zip</a> (749.1M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/country-kitchen_0.zip">country-kitchen_0.zip</a> (2.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/cozykitchen_0.zip">cozykitchen_0.zip</a> (984.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/designer-bedroom_0.zip">designer-bedroom_0.zip</a> (1.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/diner_0.zip">diner_0.zip</a> (691.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/diningroom_0.zip">diningroom_0.zip</a> (727.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/domestic-office-table_0.zip">domestic-office-table_0.zip</a> (1004.2M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/gaffer_0.zip">gaffer_0.zip</a> (858.0M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/game-room_0.zip">game-room_0.zip</a> (846.9M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/interior-scene_0.zip">interior-scene_0.zip</a> (912.5M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/italianflat_0.zip">italianflat_0.zip</a> (2.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/junkshop_0.zip">junkshop_0.zip</a> (2.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/kitchen1_0.zip">kitchen1_0.zip</a> (805.5M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/kitchen2_0.zip">kitchen2_0.zip</a> (661.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/kitchen3_0.zip">kitchen3_0.zip</a> (416.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/kitchenpack_0.zip">kitchenpack_0.zip</a> (597.2M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/lazienka_0.zip">lazienka_0.zip</a> (775.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/library-homeoffice_0.zip">library-homeoffice_0.zip</a> (909.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/livingroom_0.zip">livingroom_0.zip</a> (654.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/loft_0.zip">loft_0.zip</a> (714.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/lynxsdesign_0.zip">lynxsdesign_0.zip</a> (860.1M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/mesa-concept_0.zip">mesa-concept_0.zip</a> (465.0M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/minimarket_0.zip">minimarket_0.zip</a> (948.0M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/modern-kitchen_0.zip">modern-kitchen_0.zip</a> (645.1M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/morning-apartment_0.zip">morning-apartment_0.zip</a> (630.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/officebuilding_0.zip">officebuilding_0.zip</a> (872.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/paneled-room-revisited_0.zip">paneled-room-revisited_0.zip</a> (743.5M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/restaurant_0.zip">restaurant_0.zip</a> (2.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/restroom_0.zip">restroom_0.zip</a> (952.2M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/simplekitchen_0.zip">simplekitchen_0.zip</a> (611.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/staircase_0.zip">staircase_0.zip</a> (386.5M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/stone-shower_0.zip">stone-shower_0.zip</a> (473.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/sunny-room_0.zip">sunny-room_0.zip</a> (761.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/tv-couch_0.zip">tv-couch_0.zip</a> (572.5M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/ultramodern_0.zip">ultramodern_0.zip</a> (827.1M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/white-room_0.zip">white-room_0.zip</a> (2.1G)
        &#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/frames/datasets/renders/wooden-staircase_0.zip">wooden-staircase_0.zip</a> (853.2M)
</pre>

<pre>
Tree<'depths.json'>
&#x2570;&#x2500;&#x2500; 📁 datasets (115.4G)
    &#x2570;&#x2500;&#x2500; 📁 renders (115.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/attic_0.zip">attic_0.zip</a> (1.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/bachelors-quarters_0.zip">bachelors-quarters_0.zip</a> (2.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/barbershop_0.zip">barbershop_0.zip</a> (2.5G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/bath_0.zip">bath_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/bathroom1_0.zip">bathroom1_0.zip</a> (3.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/bathroom2_0.zip">bathroom2_0.zip</a> (2.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/bathroom3_0.zip">bathroom3_0.zip</a> (2.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/bathroom4_0.zip">bathroom4_0.zip</a> (2.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/bathroom5_0.zip">bathroom5_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/bathtime_0.zip">bathtime_0.zip</a> (1.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/bedroom1_0.zip">bedroom1_0.zip</a> (2.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/bedroom2_0.zip">bedroom2_0.zip</a> (2.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/classroom_0.zip">classroom_0.zip</a> (2.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/cocina-ii_0.zip">cocina-ii_0.zip</a> (2.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/country-kitchen_0.zip">country-kitchen_0.zip</a> (5.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/cozykitchen_0.zip">cozykitchen_0.zip</a> (2.5G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/designer-bedroom_0.zip">designer-bedroom_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/diner_0.zip">diner_0.zip</a> (2.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/diningroom_0.zip">diningroom_0.zip</a> (1.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/domestic-office-table_0.zip">domestic-office-table_0.zip</a> (1.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/gaffer_0.zip">gaffer_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/game-room_0.zip">game-room_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/interior-scene_0.zip">interior-scene_0.zip</a> (2.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/italianflat_0.zip">italianflat_0.zip</a> (4.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/junkshop_0.zip">junkshop_0.zip</a> (3.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/kitchen1_0.zip">kitchen1_0.zip</a> (2.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/kitchen2_0.zip">kitchen2_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/kitchen3_0.zip">kitchen3_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/kitchenpack_0.zip">kitchenpack_0.zip</a> (2.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/lazienka_0.zip">lazienka_0.zip</a> (1.7G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/library-homeoffice_0.zip">library-homeoffice_0.zip</a> (2.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/livingroom_0.zip">livingroom_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/loft_0.zip">loft_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/lynxsdesign_0.zip">lynxsdesign_0.zip</a> (1.8G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/mesa-concept_0.zip">mesa-concept_0.zip</a> (1.8G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/minimarket_0.zip">minimarket_0.zip</a> (2.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/modern-kitchen_0.zip">modern-kitchen_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/morning-apartment_0.zip">morning-apartment_0.zip</a> (2.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/officebuilding_0.zip">officebuilding_0.zip</a> (1.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/paneled-room-revisited_0.zip">paneled-room-revisited_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/restaurant_0.zip">restaurant_0.zip</a> (3.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/restroom_0.zip">restroom_0.zip</a> (2.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/simplekitchen_0.zip">simplekitchen_0.zip</a> (1.8G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/staircase_0.zip">staircase_0.zip</a> (2.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/stone-shower_0.zip">stone-shower_0.zip</a> (2.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/sunny-room_0.zip">sunny-room_0.zip</a> (2.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/tv-couch_0.zip">tv-couch_0.zip</a> (2.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/ultramodern_0.zip">ultramodern_0.zip</a> (2.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/white-room_0.zip">white-room_0.zip</a> (4.0G)
        &#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/depths/datasets/renders/wooden-staircase_0.zip">wooden-staircase_0.zip</a> (2.2G)
</pre>

<pre>
Tree<'normals.json'>
&#x2570;&#x2500;&#x2500; 📁 datasets (296.2G)
    &#x2570;&#x2500;&#x2500; 📁 renders (296.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/attic_0.zip">attic_0.zip</a> (5.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/bachelors-quarters_0.zip">bachelors-quarters_0.zip</a> (5.5G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/barbershop_0.zip">barbershop_0.zip</a> (8.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/bath_0.zip">bath_0.zip</a> (7.7G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/bathroom1_0.zip">bathroom1_0.zip</a> (9.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/bathroom2_0.zip">bathroom2_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/bathroom3_0.zip">bathroom3_0.zip</a> (8.5G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/bathroom4_0.zip">bathroom4_0.zip</a> (8.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/bathroom5_0.zip">bathroom5_0.zip</a> (4.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/bathtime_0.zip">bathtime_0.zip</a> (7.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/bedroom1_0.zip">bedroom1_0.zip</a> (8.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/bedroom2_0.zip">bedroom2_0.zip</a> (7.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/classroom_0.zip">classroom_0.zip</a> (9.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/cocina-ii_0.zip">cocina-ii_0.zip</a> (6.7G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/country-kitchen_0.zip">country-kitchen_0.zip</a> (10.7G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/cozykitchen_0.zip">cozykitchen_0.zip</a> (8.8G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/designer-bedroom_0.zip">designer-bedroom_0.zip</a> (8.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/diner_0.zip">diner_0.zip</a> (6.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/diningroom_0.zip">diningroom_0.zip</a> (4.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/domestic-office-table_0.zip">domestic-office-table_0.zip</a> (8.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/gaffer_0.zip">gaffer_0.zip</a> (7.8G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/game-room_0.zip">game-room_0.zip</a> (3.5G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/interior-scene_0.zip">interior-scene_0.zip</a> (7.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/italianflat_0.zip">italianflat_0.zip</a> (7.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/junkshop_0.zip">junkshop_0.zip</a> (9.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/kitchen1_0.zip">kitchen1_0.zip</a> (7.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/kitchen2_0.zip">kitchen2_0.zip</a> (2.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/kitchen3_0.zip">kitchen3_0.zip</a> (3.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/kitchenpack_0.zip">kitchenpack_0.zip</a> (573.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/lazienka_0.zip">lazienka_0.zip</a> (1.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/library-homeoffice_0.zip">library-homeoffice_0.zip</a> (7.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/livingroom_0.zip">livingroom_0.zip</a> (3.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/loft_0.zip">loft_0.zip</a> (2.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/lynxsdesign_0.zip">lynxsdesign_0.zip</a> (7.8G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/mesa-concept_0.zip">mesa-concept_0.zip</a> (2.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/minimarket_0.zip">minimarket_0.zip</a> (5.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/modern-kitchen_0.zip">modern-kitchen_0.zip</a> (3.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/morning-apartment_0.zip">morning-apartment_0.zip</a> (2.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/officebuilding_0.zip">officebuilding_0.zip</a> (6.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/paneled-room-revisited_0.zip">paneled-room-revisited_0.zip</a> (4.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/restaurant_0.zip">restaurant_0.zip</a> (10.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/restroom_0.zip">restroom_0.zip</a> (8.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/simplekitchen_0.zip">simplekitchen_0.zip</a> (4.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/staircase_0.zip">staircase_0.zip</a> (1.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/stone-shower_0.zip">stone-shower_0.zip</a> (3.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/sunny-room_0.zip">sunny-room_0.zip</a> (5.5G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/tv-couch_0.zip">tv-couch_0.zip</a> (4.5G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/ultramodern_0.zip">ultramodern_0.zip</a> (7.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/white-room_0.zip">white-room_0.zip</a> (4.4G)
        &#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/normals/datasets/renders/wooden-staircase_0.zip">wooden-staircase_0.zip</a> (6.9G)
</pre>

<pre>
Tree<'flows.json'>
&#x2570;&#x2500;&#x2500; 📁 datasets (459.4G)
    &#x2570;&#x2500;&#x2500; 📁 renders (459.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/attic_0.zip">attic_0.zip</a> (9.7G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/bachelors-quarters_0.zip">bachelors-quarters_0.zip</a> (10.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/barbershop_0.zip">barbershop_0.zip</a> (10.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/bath_0.zip">bath_0.zip</a> (10.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/bathroom1_0.zip">bathroom1_0.zip</a> (11.7G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/bathroom2_0.zip">bathroom2_0.zip</a> (9.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/bathroom3_0.zip">bathroom3_0.zip</a> (8.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/bathroom4_0.zip">bathroom4_0.zip</a> (8.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/bathroom5_0.zip">bathroom5_0.zip</a> (7.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/bathtime_0.zip">bathtime_0.zip</a> (9.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/bedroom1_0.zip">bedroom1_0.zip</a> (7.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/bedroom2_0.zip">bedroom2_0.zip</a> (8.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/classroom_0.zip">classroom_0.zip</a> (4.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/cocina-ii_0.zip">cocina-ii_0.zip</a> (9.8G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/country-kitchen_0.zip">country-kitchen_0.zip</a> (13.7G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/cozykitchen_0.zip">cozykitchen_0.zip</a> (8.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/designer-bedroom_0.zip">designer-bedroom_0.zip</a> (8.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/diner_0.zip">diner_0.zip</a> (10.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/diningroom_0.zip">diningroom_0.zip</a> (9.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/domestic-office-table_0.zip">domestic-office-table_0.zip</a> (9.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/gaffer_0.zip">gaffer_0.zip</a> (9.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/game-room_0.zip">game-room_0.zip</a> (7.8G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/interior-scene_0.zip">interior-scene_0.zip</a> (8.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/italianflat_0.zip">italianflat_0.zip</a> (9.7G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/junkshop_0.zip">junkshop_0.zip</a> (9.1G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/kitchen1_0.zip">kitchen1_0.zip</a> (8.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/kitchen2_0.zip">kitchen2_0.zip</a> (9.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/kitchen3_0.zip">kitchen3_0.zip</a> (7.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/kitchenpack_0.zip">kitchenpack_0.zip</a> (5.5G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/lazienka_0.zip">lazienka_0.zip</a> (9.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/library-homeoffice_0.zip">library-homeoffice_0.zip</a> (7.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/livingroom_0.zip">livingroom_0.zip</a> (10.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/loft_0.zip">loft_0.zip</a> (9.7G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/lynxsdesign_0.zip">lynxsdesign_0.zip</a> (8.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/mesa-concept_0.zip">mesa-concept_0.zip</a> (9.6G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/minimarket_0.zip">minimarket_0.zip</a> (10.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/modern-kitchen_0.zip">modern-kitchen_0.zip</a> (10.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/morning-apartment_0.zip">morning-apartment_0.zip</a> (9.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/officebuilding_0.zip">officebuilding_0.zip</a> (10.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/paneled-room-revisited_0.zip">paneled-room-revisited_0.zip</a> (10.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/restaurant_0.zip">restaurant_0.zip</a> (9.8G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/restroom_0.zip">restroom_0.zip</a> (7.7G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/simplekitchen_0.zip">simplekitchen_0.zip</a> (7.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/staircase_0.zip">staircase_0.zip</a> (10.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/stone-shower_0.zip">stone-shower_0.zip</a> (10.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/sunny-room_0.zip">sunny-room_0.zip</a> (5.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/tv-couch_0.zip">tv-couch_0.zip</a> (10.2G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/ultramodern_0.zip">ultramodern_0.zip</a> (10.7G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/white-room_0.zip">white-room_0.zip</a> (11.9G)
        &#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/flows/datasets/renders/wooden-staircase_0.zip">wooden-staircase_0.zip</a> (9.6G)
</pre>

<pre>
Tree<'segmentations.json'>
&#x2570;&#x2500;&#x2500; 📁 datasets (27.4G)
    &#x2570;&#x2500;&#x2500; 📁 renders (27.4G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/attic_0.zip">attic_0.zip</a> (84.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/bachelors-quarters_0.zip">bachelors-quarters_0.zip</a> (88.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/barbershop_0.zip">barbershop_0.zip</a> (168.1M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/bath_0.zip">bath_0.zip</a> (67.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/bathroom1_0.zip">bathroom1_0.zip</a> (2.9G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/bathroom2_0.zip">bathroom2_0.zip</a> (38.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/bathroom3_0.zip">bathroom3_0.zip</a> (608.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/bathroom4_0.zip">bathroom4_0.zip</a> (712.1M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/bathroom5_0.zip">bathroom5_0.zip</a> (644.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/bathtime_0.zip">bathtime_0.zip</a> (522.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/bedroom1_0.zip">bedroom1_0.zip</a> (127.0M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/bedroom2_0.zip">bedroom2_0.zip</a> (720.1M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/classroom_0.zip">classroom_0.zip</a> (161.8M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/cocina-ii_0.zip">cocina-ii_0.zip</a> (61.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/country-kitchen_0.zip">country-kitchen_0.zip</a> (5.0G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/cozykitchen_0.zip">cozykitchen_0.zip</a> (833.0M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/designer-bedroom_0.zip">designer-bedroom_0.zip</a> (655.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/diner_0.zip">diner_0.zip</a> (90.1M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/diningroom_0.zip">diningroom_0.zip</a> (75.2M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/domestic-office-table_0.zip">domestic-office-table_0.zip</a> (90.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/gaffer_0.zip">gaffer_0.zip</a> (160.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/game-room_0.zip">game-room_0.zip</a> (702.8M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/interior-scene_0.zip">interior-scene_0.zip</a> (176.5M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/italianflat_0.zip">italianflat_0.zip</a> (2.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/junkshop_0.zip">junkshop_0.zip</a> (2.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/kitchen1_0.zip">kitchen1_0.zip</a> (703.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/kitchen2_0.zip">kitchen2_0.zip</a> (80.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/kitchen3_0.zip">kitchen3_0.zip</a> (81.0M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/kitchenpack_0.zip">kitchenpack_0.zip</a> (187.9M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/lazienka_0.zip">lazienka_0.zip</a> (58.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/library-homeoffice_0.zip">library-homeoffice_0.zip</a> (143.2M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/livingroom_0.zip">livingroom_0.zip</a> (68.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/loft_0.zip">loft_0.zip</a> (47.8M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/lynxsdesign_0.zip">lynxsdesign_0.zip</a> (695.1M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/mesa-concept_0.zip">mesa-concept_0.zip</a> (49.9M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/minimarket_0.zip">minimarket_0.zip</a> (89.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/modern-kitchen_0.zip">modern-kitchen_0.zip</a> (72.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/morning-apartment_0.zip">morning-apartment_0.zip</a> (108.8M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/officebuilding_0.zip">officebuilding_0.zip</a> (119.4M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/paneled-room-revisited_0.zip">paneled-room-revisited_0.zip</a> (96.1M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/restaurant_0.zip">restaurant_0.zip</a> (2.3G)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/restroom_0.zip">restroom_0.zip</a> (113.8M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/simplekitchen_0.zip">simplekitchen_0.zip</a> (120.9M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/staircase_0.zip">staircase_0.zip</a> (56.6M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/stone-shower_0.zip">stone-shower_0.zip</a> (65.2M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/sunny-room_0.zip">sunny-room_0.zip</a> (149.2M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/tv-couch_0.zip">tv-couch_0.zip</a> (49.1M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/ultramodern_0.zip">ultramodern_0.zip</a> (592.7M)
        &#x251c;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/white-room_0.zip">white-room_0.zip</a> (2.2G)
        &#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/segmentations/datasets/renders/wooden-staircase_0.zip">wooden-staircase_0.zip</a> (100.1M)
</pre>

<pre>
Tree<'previews.json'>
&#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/previews/datasets_0.zip">datasets_0.zip</a> (6.5G)
</pre>

<pre>
Tree<'metadata.json'>
&#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/visionsim/visionsim50/metadata/datasets_0.zip">datasets_0.zip</a> (51.2M)
</pre>

*Note:* The zip file sizes refer to the decompressed filesize.

*Tip:* To see the full details for each split, you can use the `show-tree` like so `uv run tools.py show-tree trees/visionsim50/frames.json --full`. To generate the tree with direct download links, use `uv run tools.py show-tree trees/visionsim50/frames.json --s3-prefix visionsim/visionsim50/frames`. Use `--html` to render links as HTML `<a>` tags (needed inside `<details>` tags).

</details>

## Quanta Neural Networks

We include high-speed video sequences (at framerates of about 16 kFPS) and groundtruth annotations for three computer vision tasks: monocular depth estimation, multi-frame point tracking, and video restoration (for which the input high-speed sequences are the groundtruth itself). These high-speed sequences were used to simulate photon detections and train [Quanta Neural Networks](https://wisionlab.com/project/quanta-neural-networks/), published at ICCV 2025.

To download these datasets, please use `DATASET_PREFIX=quanta-neural-networks`.

<details>
<summary>See Detailed Folder Structure</summary>

<pre>
Tree<'blender_depth_faster.json'>
&#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/quanta-neural-networks/blender_depth_faster/blender_depth_faster_0.zip">blender_depth_faster_0.zip</a> (22.3G 1.1x)
</pre>

<pre>
Tree<'tracking.json'>
&#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/quanta-neural-networks/tracking/tracking_dataset_0.zip">tracking_dataset_0.zip</a> (8.7G 1.8x)
</pre>

<pre>
Tree<'xvfi.json'>
&#x2570;&#x2500;&#x2500; 💾 <a href="https://web.s3.wisc.edu/public-datasets/quanta-neural-networks/xvfi/xvfi_0.zip">xvfi_0.zip</a> (1.7G 1.0x)
</pre>

*Note:* The zip file sizes refer to the decompressed filesize.

*Tip:* To see the full details for each split, you can use the `show-tree` like so `uv run tools.py show-tree trees/quanta-neural-networks/xvfi.json --full`. To generate the tree with direct download links, use `uv run tools.py show-tree trees/quanta-neural-networks/xvfi.json --s3-prefix quanta-neural-networks`. Use `--html` to render links as HTML `<a>` tags (needed inside `<details>` tags).

</details>