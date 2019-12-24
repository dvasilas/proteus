import matplotlib.pyplot as plt

client_num = [1, 2, 4, 8, 16, 32, 64, 128, 256]
x = list(range(len(client_num)))

local_index_query_heavy = [7.744211661, 11.53680635, 19.43989099, 31.46246271, 56.82806601, 107.7279634, 209.6756666, 396.5661872, 727.0732134]
remote_index_query_heavy = [97.20937278, 96.2942439, 97.02689195, 98.0927206, 104.2649619, 129.4193965, 214.144991, 349.7850205, 786.9735419]
remote_index_cache_query_heavy = [70.08381429, 67.90770539, 49.55152445, 9.397477456, 4.876919545, 6.103077657, 9.516440544, 24.71773381, 35.72532888]

local_index_balanced = [12.2231085, 14.04865422, 18.45247485, 28.99362538, 50.92978905, 93.64701892, 216.6404905, 356.8365347, 830.1514062]
remote_index_balanced = [99.03441048, 99.23213749, 101.4613042, 105.2770851, 112.0072762, 133.1493236, 201.8492082, 388.5722381, 786.2691796]
remote_index_cache_balanced = [71.26192038, 72.52375324, 58.04157734, 28.01980351, 14.27468311, 11.14413233, 10.33961246, 10.52911323, 11.30313182]

local_index_update_heavy = [13.44444892, 14.28448373, 15.37405908, 18.89647287, 18.18719546, 17.08161084, 16.24058151, 15.06462919, 17.35911907]
remote_index_update_heavy = [103.3428013, 102.2961067, 103.9401247, 105.6454387, 106.8120248, 104.9732084, 106.60099, 106.363469, 104.5900155]
remote_index_cache_update_heavy = [81.06599448, 77.58478313, 75.09661406, 65.31818045, 59.8986329, 60.09339085, 57.91501465, 58.50940827, 57.67304793]

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
    ax.set(xlabel='Number of client threads', ylabel='Average response time (ms)')

handles, labels = axarr[0].get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=3, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('latency_clients.png')
plt.close()