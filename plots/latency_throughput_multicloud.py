import matplotlib.pyplot as plt

client_num = [1, 2, 4, 8, 16, 32, 64, 128, 256]
x = list(range(len(client_num)))

central_index_query_heavy_throughput = [90.6802071, 124.1766355, 156.3608934, 185.5048215, 202.9709211, 205.726215, 230.3216724, 206.1633684, 177.1810563]
central_index_query_heavy_latency = [32.16543664, 47.36360306, 75.81375193, 128.4018688, 235.6413353, 465.8260754, 831.2892238, 1849.888134, 4176.173498]
distributed_index_query_heavy_throughput = [14.09939401, 28.1980317, 56.08874568, 112.7905466, 214.7479165, 250.7853734, 286.3595933, 279.2117714, 241.9080256]
distributed_index_query_heavy_latency = [211.8505305, 211.8358256, 213.0607188, 211.9202834, 222.5808445, 381.2039854, 668.932884, 1359.77021, 3143.6441]
distributed_index_cache_query_heavy_throughput = [22.10400979, 41.01779025, 129.2073104, 322.3160419, 488.2984243, 568.377647, 641.4963587, 615.4464537, 600.2430259]
distributed_index_cache_query_heavy_latency = [134.9292054, 145.4309013, 91.94640492, 73.5871803, 97.47442428, 168.1071839, 298.9727092, 625.5484308, 1298.083223]

central_index_balanced_throughput = [53.95979133, 95.84772688, 138.7499817, 174.0980591, 189.4723849, 203.4714455, 207.1333229, 273.386588, 162.4640559]
central_index_balanced_latency = [49.90253287, 56.29307814, 80.00622424, 131.0867608, 246.9226502, 463.9466535, 918.3380121, 1395.335749, 4626.028821]
distributed_index_balanced_throughput = [14.04879986, 27.74344814, 55.70136959, 111.1510695, 184.121053, 257.2511575, 304.3037864, 288.9401504, 273.3839741]
distributed_index_balanced_latency = [206.8953281, 209.5947454, 208.8441276, 209.2201735, 254.2308078, 366.2989139, 622.8171592, 1318.273677, 2763.254375]
distributed_index_cache_balanced_throughput = [17.54137277, 40.76317393, 97.99931046, 242.7143632, 410.29073, 545.4579716, 657.1825102, 628.7713851, 615.0928482]
distributed_index_cache_balanced_latency = [163.9593797, 140.7107713, 116.0989908, 92.11790125, 109.6590504, 167.7586976, 280.7770321, 598.1814271, 1238.196494]

central_index_update_heavy_throughput = [23.99613865, 45.63107476, 81.07322018, 114.3399293, 137.0867888, 140.8950032, 142.5561389, 84.82136664, 34.52618194]
central_index_update_heavy_latency = [71.39022862, 77.16371654, 87.3226056, 128.6595063, 233.0920634, 475.8898684, 1003.052593, 2110.348814, 5784.691253]
distributed_index_update_heavy_throughput = [11.2388645, 22.74241865, 42.08544397, 81.98361456, 138.5665796, 171.9286946, 179.9575111, 179.1355375, 183.0193111]
distributed_index_update_heavy_latency = [213.3851754, 208.7511845, 227.9306603, 227.2572734, 243.7010836, 320.5572888, 426.9997536, 463.3432885, 627.8555806]
distributed_index_cache_update_heavy_throughput = [13.90714848, 27.93480788, 58.9883655, 120.9278382, 165.3954469, 181.6744113, 185.8570124, 185.7236736, 185.5291067]
distributed_index_cache_update_heavy_latency = [161.0738602, 161.2311456, 147.56257, 121.2116757, 123.269561, 100.9929168, 99.7455898, 104.0374866, 95.39359884]

plt.close('all')

f, axarr = plt.subplots(1, 3, figsize=(15,5))

axarr[0].plot(central_index_query_heavy_throughput, central_index_query_heavy_latency, color='b', marker='+', linewidth=2, label='central-index')
axarr[0].plot(distributed_index_query_heavy_throughput, distributed_index_query_heavy_latency, color='r', marker='x', linewidth=2, label='distributed-index')
axarr[0].plot(distributed_index_cache_query_heavy_throughput, distributed_index_cache_query_heavy_latency, color='y', marker='^', linewidth=2, label='distributed-index-cache')
axarr[0].set_title('Query-heavy')

axarr[1].plot(central_index_balanced_throughput, central_index_balanced_latency, color='b', marker='+', linewidth=2, label='central-index')
axarr[1].plot(distributed_index_balanced_throughput, distributed_index_balanced_latency, color='r', marker='x', linewidth=2, label='distributed-index')
axarr[1].plot(distributed_index_cache_balanced_throughput, distributed_index_cache_balanced_latency, color='y', marker='^' ,linewidth=2, label='distributed-index-cache')
axarr[1].set_title('Balanced')

axarr[2].plot(central_index_update_heavy_throughput, central_index_update_heavy_latency, color='b', marker='+', linewidth=2, label='central-index',)
axarr[2].plot(distributed_index_update_heavy_throughput, distributed_index_update_heavy_latency, color='r', marker='x', linewidth=2, label='distributed-index')
axarr[2].plot(distributed_index_cache_update_heavy_throughput, distributed_index_cache_update_heavy_latency, color='y', marker='^', linewidth=2, label='distributed-index-cache')
axarr[2].set_title('Update-heavy')

for ax in axarr.flat:
    ax.set(xlabel='Query throughput (ops/s)', ylabel='Average response time (ms)')

handles, labels = axarr[0].get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=3, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('latency_throughput_multicloud.png')
plt.close()