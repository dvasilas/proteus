import matplotlib.pyplot as plt

client_num = [1, 2, 4, 8, 16, 32, 64, 128, 256]
x = list(range(len(client_num)))

local_query_heavy = [49.99574942, 70.13357466, 468.7960515, 14290.82708, 21176.28461, 25266.68982, 28550.26726, 31643.68331, 31326.35429]
remote_query_query_heavy = [98.42849315, 101.8433208, 124.5497778, 146.5816642, 2452.586483, 21763.3981, 29638.44733, 34956.06993, 32093.2352]
remote_update_cache_query_heavy = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_update_query_heavy = [105.2742235, 980.6756564, 10683.69497, 16177.15588, 20089.66855, 23961.73591, 27437.18854, 33005.7133, 25436.0257]

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
    ax.set(xlabel='Number of client threads', ylabel='Average freshness latency (ms)')

handles, labels = axarr[0].get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=4, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('freshness_clients.png')
plt.close()

print(pow(2,38))