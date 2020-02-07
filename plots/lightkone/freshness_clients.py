import matplotlib.pyplot as plt

client_num = [1, 2, 4, 8, 16, 32, 64, 128, 256]
x = list(range(len(client_num)))

local_index_query_heavy = [37.29818731, 68.02978497, 7115.495264, 17916.55861, 24701.44803, 25016.9554, 28353.86811, 30358.46542, 33954.92571]
remote_index_query_heavy = [301.1550476, 303.2899048, 313.8601804, 389.7689084, 18153.62894, 27612.61929, 32116.62171, 28861.09496, 32373.23093]
remote_index_cache_query_heavy = [301.7822921, 305.1192066, 311.7213351, 335.2500282, 11521.83305, 20662.23189, 23483.12848, 24758.19462, 25456.66997]

local_index_balanced = [5643.69472, 17754.98974, 22831.52091, 26490.50669, 26094.2162, 23883.94443, 25446.22833, 22435.1107, 32933.62317]
remote_index_balanced = [299.4180971, 297.7038841, 300.5228468, 312.8507918, 7779.730277, 19423.95875, 21405.14385, 24083.10766, 26827.20869]
remote_index_cache_balanced = [298.5805049, 298.5345542, 303.3984463, 317.1716129, 7459.543368, 19033.07518, 25531.71597, 25788.80852, 26887.12485]

local_index_update_heavy = [16349.6616, 21401.68634, 23531.63689, 10103.43572, 24689.61541, 24125.91524, 22138.61125, 19081.92687, 21699.30079]
remote_index_update_heavy = [296.8609811, 298.3746259, 297.4046551, 316.9906605, 4545.670475, 16401.66789, 21073.21602, 21482.63003, 22436.99607]
remote_index_cache_update_heavy = [296.5195117, 295.9995524, 299.5034598, 318.5917032, 4561.107327, 16311.17542, 21151.55878, 21478.79138, 22480.32273]

plt.close('all')

f, axarr = plt.subplots(1, 3, figsize=(15,5))

plt.setp(axarr, xticks=x, xticklabels=client_num)

axarr[0].plot(x, local_index_query_heavy, color='b', marker='+', linewidth=2, label='local-index')
axarr[0].plot(x, remote_index_query_heavy, color='r', marker='x', linewidth=2, label='remote-index')
axarr[0].plot(x, remote_index_cache_query_heavy, color='y', marker='^', linewidth=2, label='remote-index-cache')
axarr[0].set_title('Query-heavy')

axarr[1].plot(x, local_index_balanced, color='b', marker='+', linewidth=2, label='local-index',)
axarr[1].plot(x, remote_index_balanced, color='r', marker='x', linewidth=2, label='remote-index')
axarr[1].plot(x, remote_index_cache_balanced, color='y', marker='^', linewidth=2, label='remote-index-cache')
axarr[1].set_title('Balanced')

axarr[2].plot(x, local_index_update_heavy, color='b', marker='+', linewidth=2, label='local-index',)
axarr[2].plot(x, remote_index_update_heavy, color='r', marker='x', linewidth=2, label='remote-index')
axarr[2].plot(x, remote_index_cache_update_heavy, color='y', marker='^', linewidth=2, label='remote-index-cache')
axarr[2].set_title('Update-heavy')

for ax in axarr.flat:
    ax.set(xlabel='Number of client threads', ylabel='Average freshness latency (ms)')

handles, labels = axarr[0].get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=3, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('freshness_clients.png')
plt.close()