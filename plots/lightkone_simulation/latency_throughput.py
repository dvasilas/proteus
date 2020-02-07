import matplotlib.pyplot as plt

local_query_heavy_throughput = [66.03207615, 109.2065524, 138.585164, 198.1682931, 225.0954218, 258.999122, 247.4979606, 245.0169572, 219.4381912]
local_query_heavy_latency = [13.80104332, 17.07802967, 27.62492658, 39.23129344, 69.89624663, 122.4073017, 258.2401515, 521.9390636, 1173.554389]
remote_query_query_heavy_throughput = [11.86528704, 23.79667925, 45.29658478, 88.08445252, 145.3276154, 216.7338307, 273.3435687, 300.8610233, 251.1161373]
remote_query_query_heavy_latency = [82.537, 82.58377834, 86.80203175, 89.60219665, 108.7880099, 146.6409573, 232.9461066, 426.5132373, 1024.467744]
remote_update_cache_query_heavy_throughput = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_update_cache_query_heavy_latency = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_update_query_heavy_latency = [8.643589488, 10.19139665, 14.1110266, 22.7630068, 40.333561, 68.58033924, 131.7000212, 259.7517176, 505.675534]
remote_update_query_heavy_throughput = [100.4620092, 175.3814161, 261.5886094, 335.7018474, 382.7903258, 458.2008678, 481.8082028, 491.7879024, 503.8263283]

local_balanced_throughput = [0, 0, 0, 0, 0, 0, 0, 0, 0]
local_balanced_latency = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_balanced_throughput = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_balanced_latency = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_cache_balanced_throughput = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_cache_balanced_latency = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_update_balanced_throughput = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_update_balanced_latency = [0, 0, 0, 0, 0, 0, 0, 0, 0]

local_update_heavy_throughput = [0, 0, 0, 0, 0, 0, 0, 0, 0]
local_update_heavy_latency = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_update_heavy_throughput = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_update_heavy_latency = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_cache_update_heavy_throughput = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_cache_update_heavy_latency = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_update_update_heavy_throughput = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_update_update_heavy_latency = [0, 0, 0, 0, 0, 0, 0, 0, 0]

plt.close('all')

f, axarr = plt.subplots(1, 3, figsize=(15,5))

axarr[0].plot(local_query_heavy_throughput, local_query_heavy_latency, color='b', marker='+', linewidth=2, label='local')
axarr[0].plot(remote_query_query_heavy_throughput, remote_query_query_heavy_latency, color='r', marker='x', linewidth=2, label='remote-query')
axarr[0].plot(remote_update_cache_query_heavy_throughput, remote_update_cache_query_heavy_latency, color='y', marker='^', linewidth=2, label='remote-query-cache')
axarr[0].plot(remote_update_query_heavy_throughput, remote_update_query_heavy_latency, color='g', marker='o', linewidth=2, label='remote-update')
axarr[0].set_title('Query-heavy')

axarr[1].plot(local_balanced_throughput, local_balanced_latency, color='b', marker='+', linewidth=2, label='local',)
axarr[1].plot(remote_query_balanced_throughput, remote_query_balanced_latency, color='r', marker='x', linewidth=2, label='remote-query')
axarr[1].plot(remote_query_cache_balanced_throughput, remote_query_cache_balanced_latency, color='y', marker='^', linewidth=2, label='remote-query-cache')
axarr[1].plot(remote_update_balanced_latency, remote_update_balanced_latency, color='g', marker='o', linewidth=2, label='remote-update')
axarr[1].set_title('Balanced')

axarr[2].plot(local_update_heavy_throughput, local_update_heavy_latency, color='b', marker='+', linewidth=2, label='local',)
axarr[2].plot(remote_query_update_heavy_throughput, remote_query_update_heavy_latency, color='r', marker='x', linewidth=2, label='remote-query')
axarr[2].plot(remote_query_cache_update_heavy_throughput, remote_query_cache_update_heavy_latency, color='y', marker='^', linewidth=2, label='remote-query-cache')
axarr[2].plot(remote_update_update_heavy_throughput, remote_update_update_heavy_latency, color='g', marker='o', linewidth=2, label='remote-update')
axarr[2].set_title('Update-heavy')

for ax in axarr.flat:
    ax.set(xlabel='Query throughput (ops/s)', ylabel='Average response time (ms)')

handles, labels = axarr[0].get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=4, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('latency_throughput.png')
plt.close()