#!/bin/bash
sudo modprobe rdma_cm
sudo modprobe ib_uverbs
sudo modprobe ib_umad
sudo modprobe ib_ipoib
sudo modprobe mlx4_ib
sudo modprobe iw_cxgb3
sudo modprobe iw_cxgb4
sudo modprobe iw_nes
sudo modprobe iw_c2
sudo modprobe ib_mthca
