import matplotlib.pyplot as plt

client_num = [1, 2, 4, 8, 16, 32, 64, 128, 256]
x = list(range(len(client_num)))

central_index_query_heavy = [69.08108978, 198.9116249, 16868.71577, 46550.17224, 58899.97832, 62670.18382, 59211.82933, 54897.59722, 76066.95253]
distributed_index_query_heavy = [32.0688, 30.96178446, 33.94211471, 43.31873197, 2256.122194, 19714.6632, 32447.23966, 55244.95174, 38585.44775]
distributed_index_cache_query_heavy = [34.9723125, 37.9996763, 62.66360434, 3220.664903, 16551.57496, 27201.87066, 27331.21097, 49005.37061, 22742.20135]

central_index_balanced = [16305.04843, 34472.63342, 49678.66202, 47749.10524, 65870.88895, 55896.18506, 57987.51232, 66831.41689, 58176]
distributed_index_balanced = [37.88725063, 47.89621473, 87.73746628, 10600.50759, 38633.26057, 51935.56589, 51083.66157, 65754.13512, 78155.87272]
distributed_index_cache_balanced = [46.84502372, 71.31655875, 3665.887069, 20757.06962, 27348.26252, 40367.01281, 49520.03351, 37254.94896, 44803.61244]

central_index_update_heavy = [43714.33865, 49001.73593, 49784.1125, 51530.549, 46378.9472, 49996.46747, 40718.77999, 44006.53098, 48437.3363]
distributed_index_update_heavy = [619.2299838, 19271.71801, 36567.75152, 42375.32622, 47022.373, 44231.18101, 45956.28066, 42891.97183, 43147.40254]
distributed_index_cache_update_heavy = [2679.376511, 24881.36882, 38689.43597, 35414.00006, 51912.63086, 53861.08937, 54338.7634, 54885.52449, 53986.21769]

plt.close('all')

f, axarr = plt.subplots(1, 3, figsize=(15,5))

plt.setp(axarr, xticks=x, xticklabels=client_num)

axarr[0].plot(x, central_index_query_heavy, color='b', marker='+', linewidth=2, label='central-index')
axarr[0].plot(x, distributed_index_query_heavy, color='r', marker='x', linewidth=2, label='distributed-index')
axarr[0].plot(x, distributed_index_cache_query_heavy, color='y', marker='^', linewidth=2, label='distributed-index-cache')
axarr[0].set_title('Query-heavy')

axarr[1].plot(x, central_index_balanced, color='b', marker='+', linewidth=2, label='central-index',)
axarr[1].plot(x, distributed_index_balanced, color='r', marker='x', linewidth=2, label='distributed-index')
axarr[1].plot(x, distributed_index_cache_balanced, color='y', marker='^', linewidth=2, label='distributed-index-cache')
axarr[1].set_title('Balanced')

axarr[2].plot(x, central_index_update_heavy, color='b', marker='+', linewidth=2, label='central-index',)
axarr[2].plot(x, distributed_index_update_heavy, color='r', marker='x', linewidth=2, label='distributed-index')
axarr[2].plot(x, distributed_index_cache_update_heavy, color='y', marker='^', linewidth=2, label='distributed-index-cache')
axarr[2].set_title('Update-heavy')

for ax in axarr.flat:
    ax.set(xlabel='Number of client threads', ylabel='Average freshness latency (ms)')

handles, labels = axarr[0].get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=3, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('freshness_clients_multicloud.png')
plt.close()

# ==========================================================================================================================================

