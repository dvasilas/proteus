import matplotlib.pyplot as plt

client_num = [1, 2, 4, 8, 16, 32, 64, 128, 256]
x = list(range(len(client_num)))

local_query_heavy = [13.80104332, 17.07802967, 27.62492658, 39.23129344, 69.89624663, 122.4073017, 258.2401515, 521.9390636, 1173.554389]
remote_query_query_heavy = [82.537, 82.58377834, 86.80203175, 89.60219665, 108.7880099, 146.6409573, 232.9461066, 426.5132373, 1024.467744]
remote_update_cache_query_heavy = [0, 0, 0, 0, 0, 0, 0, 0, 0]
remote_update_query_heavy = [8.643589488, 10.19139665, 14.1110266, 22.7630068, 40.333561, 68.58033924, 131.7000212, 259.7517176, 505.675534]

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
    ax.set(xlabel='Number of client threads', ylabel='Average response time (ms)')

handles, labels = axarr[0].get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=4, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('latency_clients.png')
plt.close()

