import matplotlib.pyplot as plt

client_num = [1, 2, 4, 8, 16, 32, 64, 128, 256]
x = list(range(len(client_num)))

local_index_query_heavy_throughput = [113.1954722, 158.8050629, 195.5408918, 244.3, 275.986737, 294.2092341, 303.9811806, 322.9438091, 353.4242238]
local_index_query_heavy_latency = [7.744211661, 11.53680635, 19.43989099, 31.46246271, 56.82806601, 107.7279634, 209.6756666, 396.5661872, 727.0732134]
remote_index_query_heavy_throughput = [10.12299337, 20.50082003, 40.64440141, 80.48410289, 151.7547012, 244.8784185, 297.2267602, 364.6260399, 325.646084]
remote_index_query_heavy_latency = [97.20937278, 96.2942439, 97.02689195, 98.0927206, 104.2649619, 129.4193965, 214.144991, 349.7850205, 786.9735419]
remote_index_cache_query_heavy_throughput = [13.99496181, 28.89110889, 78.92263702, 749.80008, 2276.072393, 3009.713752, 3962.304879, 4294.784355]
remote_index_cache_query_heavy_latency = [70.08381429, 67.90770539, 49.55152445, 9.397477456, 4.876919545, 6.103077657, 9.516440544, 35.72532888]

local_index_balanced_throughput = [52.72, 95.29903421, 153.0691842, 216.4620737, 259.3818544, 282.263204, 337.1757925, 304.3734296]
local_index_balanced_latency = [12.2231085, 14.04865422, 18.45247485, 28.99362538, 50.92978905, 216.6404905, 356.8365347, 830.1514062]
remote_index_balanced_throughput = [9.145550031, 18.59991209, 36.56588249, 70.54339367, 133.711297, 218.2717823, 299.1490464, 302.679103, 311.8598222]
remote_index_balanced_latency = [99.03441048, 99.23213749, 101.4613042, 105.2770851, 112.0072762, 133.1493236, 201.8492082, 388.5722381, 786.2691796]
remote_index_cache_balanced_throughput = [12.54168913, 24.70863403, 60.78936336, 212.5466856, 438.6210482, 464.9073778, 474.9076385, 489.8214534]
remote_index_cache_balanced_latency = [71.26192038, 72.52375324, 58.04157734, 28.01980351, 14.27468311, 11.14413233, 10.33961246, 11.30313182]

local_index_update_heavy_throughput = [0, 14.87672712, 27.03675559, 41.28183599, 54.54254639, 56.41046165, 58.28932629, 59.85943456]
local_index_update_heavy_latency = [13, 13.44444892, 14.28448373, 15.37405908, 18.89647287, 18.18719546, 17.08161084, 17.35911907]
remote_index_update_heavy_throughput = [0, 6.134478969, 11.99784039, 23.69639863, 39.45474896, 50.69382051, 59.69136172]
remote_index_update_heavy_latency = [103, 103.3428013, 102.2961067, 103.9401247, 105.6454387, 104.9732084, 106.60099]
remote_index_cache_update_heavy_throughput = [0, 7.241738017, 14.9167299, 26.71382046, 47.8493764, 55.43463067, 57.19645566, 57.38211544, 57.60607781]
remote_index_cache_update_heavy_latency = [81, 81.06599448, 77.58478313, 75.09661406, 65.31818045, 60.09339085, 57.91501465, 58.50940827, 57.67304793]

plt.close('all')

f, axarr = plt.subplots(1, 3, figsize=(15,5))

axarr[0].plot(local_index_query_heavy_throughput, local_index_query_heavy_latency, color='b', marker='+', linewidth=2, label='local-index')
axarr[0].plot(remote_index_query_heavy_throughput, remote_index_query_heavy_latency, color='r', marker='x', linewidth=2, label='remote-index')
axarr[0].plot(remote_index_cache_query_heavy_throughput, remote_index_cache_query_heavy_latency, color='y', marker='^', linewidth=2, label='remote-index-cache')
axarr[0].set_title('Query-heavy')

axarr[1].plot(local_index_balanced_throughput, local_index_balanced_latency, color='b', marker='+', linewidth=2, label='local-index')
axarr[1].plot(remote_index_balanced_throughput, remote_index_balanced_latency, color='r', marker='x', linewidth=2, label='remote-index')
axarr[1].plot(remote_index_cache_balanced_throughput, remote_index_cache_balanced_latency, color='y', marker='^' ,linewidth=2, label='remote-index-cache')
axarr[1].set_title('Balanced')

axarr[2].plot(local_index_update_heavy_throughput, local_index_update_heavy_latency, color='b', marker='+', linewidth=2, label='local-index',)
axarr[2].plot(remote_index_update_heavy_throughput, remote_index_update_heavy_latency, color='r', marker='x', linewidth=2, label='remote-index')
axarr[2].plot(remote_index_cache_update_heavy_throughput, remote_index_cache_update_heavy_latency, color='y', marker='^', linewidth=2, label='remote-index-cache')
axarr[2].set_title('Update-heavy')

for ax in axarr.flat:
    ax.set(xlabel='Query throughput (ops/s)', ylabel='Average response time (ms)')

handles, labels = axarr[0].get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=3, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('latency_throughput.png')
plt.close()