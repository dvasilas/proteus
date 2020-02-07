import matplotlib.pyplot as plt

client_num = [1, 2, 4, 8, 16, 32, 64, 128, 256]
x = list(range(len(client_num)))

local_index_query_heavy = [113.1954722, 158.8050629, 195.5408918, 244.3, 275.986737, 294.2092341, 303.9811806, 322.9438091, 353.4242238]
remote_index_query_heavy = [10.12299337, 20.50082003, 40.64440141, 80.48410289, 151.7547012, 244.8784185, 297.2267602, 364.6260399, 325.646084]
remote_index_cache_query_heavy = [13.99496181, 28.89110889, 78.92263702, 749.80008, 2276.072393, 3009.713752, 3962.304879, 3651.859034, 4294.784355]

local_index_balanced = [52.72, 95.29903421, 153.0691842, 216.4620737, 259.3818544, 214.0818648, 282.263204, 337.1757925, 304.3734296]
remote_index_balanced = [9.145550031, 18.59991209, 36.56588249, 70.54339367, 133.711297, 218.2717823, 299.1490464, 302.679103, 311.8598222]
remote_index_cache_balanced = [12.54168913, 24.70863403, 60.78936336, 212.5466856, 438.6210482, 464.9073778, 474.9076385, 412.9308805, 489.8214534]

local_index_update_heavy = [14.87672712, 27.03675559, 41.28183599, 54.54254639, 56.41046165, 58.28932629, 53.73747855, 48.21606538, 59.85943456]
remote_index_update_heavy = [6.134478969, 11.99784039, 23.69639863, 39.45474896, 51.53570004, 50.69382051, 59.69136172, 56.32651434, 56.33523235]
remote_index_cache_update_heavy = [7.241738017, 14.9167299, 26.71382046, 47.8493764, 56.58530739, 55.43463067, 57.19645566, 57.38211544, 57.60607781]

plt.close('all')

f, axarr = plt.subplots(2, 2, figsize=(10,10))

plt.setp(axarr, xticks=x, xticklabels=client_num)

axarr[0, 0].plot(x, local_index_query_heavy, color='b', marker='+', linewidth=2, label='local-index',)
axarr[0, 0].plot(x, remote_index_query_heavy, color='r', marker='x', linewidth=2, label='remote-index')
axarr[0, 0].plot(x, remote_index_cache_query_heavy, color='y', marker='^', linewidth=2, label='remote-index-cache')
axarr[0, 0].set_title('Query-heavy')

axarr[0, 1].plot(x, local_index_query_heavy, color='b', marker='+', linewidth=2, label='local-index',)
axarr[0, 1].plot(x, remote_index_query_heavy, color='r', marker='x', linewidth=2, label='remote-index')
axarr[0, 1].set_title('Query-heavy')

axarr[1, 0].plot(x, local_index_balanced, color='b', marker='+', linewidth=2, label='local-index',)
axarr[1, 0].plot(x, remote_index_balanced, color='r', marker='x', linewidth=2, label='remote-index')
axarr[1, 0].plot(x, remote_index_cache_balanced, color='y', marker='^', linewidth=2, label='remote-index-cache')
axarr[1, 0].set_title('Balanced')

axarr[1, 1].plot(x, local_index_update_heavy, color='b', marker='+', linewidth=2, label='local-index',)
axarr[1, 1].plot(x, remote_index_update_heavy, color='r', marker='x', linewidth=2, label='remote-index')
axarr[1, 1].plot(x, remote_index_cache_update_heavy, color='y', marker='^', linewidth=2, label='remote-index-cache')
axarr[1, 1].set_title('Update-heavy')

for ax in axarr.flat:
    ax.set(xlabel='Number of client threads', ylabel='Query throughput (ops/s)')

handles, labels = axarr[0, 0].get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=3, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('throughput_clients.png')
plt.close()