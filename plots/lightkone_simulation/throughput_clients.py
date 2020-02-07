import matplotlib.pyplot as plt

client_num = [1, 2, 4, 8, 16, 32, 64, 128, 256]
x = list(range(len(client_num)))

local_query_heavy = [66.03207615, 109.2065524, 138.585164, 198.1682931, 225.0954218, 258.999122, 247.4979606, 245.0169572, 219.4381912]
remote_query_query_heavy = [11.86528704, 23.79667925, 45.29658478, 88.08445252, 145.3276154, 216.7338307, 273.3435687, 300.8610233, 251.1161373]
remote_update_cache_query_heavy = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_update_query_heavy = [100.4620092, 175.3814161, 261.5886094, 335.7018474, 382.7903258, 458.2008678, 481.8082028, 491.7879024, 503.8263283]

local_balanced = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_balanced = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_cache_balanced = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_update_balanced = [0, 0, 0, 0, 0, 0, 0, 0, 0]

local_update_heavy = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_update_heavy = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_query_cache_update_heavy = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_update_update_heavy = [0, 0, 0, 0, 0, 0, 0, 0, 0]

plt.close('all')

f, axarr = plt.subplots(1, 3, figsize=(15,5))

plt.setp(axarr, xticks=x, xticklabels=client_num)

axarr[0].plot(x, local_query_heavy, color='b', marker='+', linewidth=2, label='local')
axarr[0].plot(x, remote_query_query_heavy, color='r', marker='x', linewidth=2, label='remote-query')
axarr[0].plot(x, remote_update_cache_query_heavy, color='y', marker='^', linewidth=2, label='remote-query-cache')
axarr[0].plot(x, remote_update_query_heavy, color='g', marker='o', linewidth=2, label='remote-update')
axarr[0].set_title('Query-heavy')

axarr[1].plot(x, local_balanced, color='b', marker='+', linewidth=2, label='local',)
axarr[1].plot(x, remote_query_balanced, color='r', marker='x', linewidth=2, label='remote-query')
axarr[1].plot(x, remote_query_cache_balanced, color='y', marker='^', linewidth=2, label='remote-query-cache')
axarr[1].plot(x, remote_update_balanced, color='g', marker='o', linewidth=2, label='remote-update')
axarr[1].set_title('Balanced')

axarr[2].plot(x, local_update_heavy, color='b', marker='+', linewidth=2, label='local',)
axarr[2].plot(x, remote_query_update_heavy, color='r', marker='x', linewidth=2, label='remote-query')
axarr[2].plot(x, remote_query_cache_update_heavy, color='y', marker='^', linewidth=2, label='remote-query-cache')
axarr[2].plot(x, remote_update_update_heavy, color='g', marker='o', linewidth=2, label='remote-update')
axarr[2].set_title('Update-heavy')

for ax in axarr.flat:
    ax.set(xlabel='Number of client threads', ylabel='Query throughput (ops/s)')

handles, labels = axarr[0].get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=4, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('throughput_clients.png')
plt.close()