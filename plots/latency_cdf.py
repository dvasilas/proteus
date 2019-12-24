import matplotlib.pyplot as plt

percentage = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99]

central_index_query_heavy_latency = [83.327, 115.519, 141.823, 168.063, 193.023, 220.031, 249.599, 285.951, 333.823, 378.111, 493.311]
distributed_index_query_heavy_latency = [150.399, 158.463, 170.367, 197.759, 236.543, 242.303, 249.087, 259.071, 276.223, 292.095, 337.663]
distributed_index_cache_query_heavy_latency = [27.727, 38.495, 48.063, 57.663, 68.031, 80.191, 96.959, 124.671, 204.671, 289.279, 361.727]

central_index_balanced_latency = [102.143, 136.191, 164.351, 188.799, 214.399, 241.151, 269.567, 302.847, 356.863, 416.767, 551.423]
distributed_index_balanced_latency = [174.335, 188.287, 208.767, 241.279, 249.983, 259.199, 270.079, 283.647, 305.151, 324.607, 367.871]
distributed_index_cache_balanced_latency = [26.815, 38.559, 49.151, 59.839, 71.615, 85.823, 105.663, 142.207, 255.999, 302.591, 387.071]

central_index_update_heavy_latency = [42.559, 100.607, 121.343, 156.543, 178.047, 202.367, 234.623, 277.247, 338.687, 393.471, 507.135]
distributed_index_update_heavy_latency = [156.287, 169.599, 192.639, 239.871, 246.271, 252.543, 260.095, 270.847, 291.071, 313.343, 385.535]
distributed_index_cache_update_heavy_latency = [13.183, 16.959, 21.679, 28.303, 38.975, 62.271, 174.847, 259.071, 274.175, 291.583, 341.247]

plt.close('all')

f, axarr = plt.subplots(1, 3, figsize=(18,4))

axarr[0].plot(central_index_query_heavy_latency, percentage, color='b', marker='+', linewidth=2, label='central-index')
axarr[0].plot(distributed_index_query_heavy_latency, percentage, color='r', marker='x', linewidth=2, label='distributed-index')
axarr[0].plot(distributed_index_cache_query_heavy_latency, percentage, color='y', marker='^', linewidth=2, label='distributed-index-cache')
axarr[0].set_title('Query-heavy')

axarr[1].plot(central_index_balanced_latency, percentage, color='b', marker='+', linewidth=2, label='central-index')
axarr[1].plot(distributed_index_balanced_latency, percentage, color='r', marker='x', linewidth=2, label='distributed-index')
axarr[1].plot(distributed_index_cache_balanced_latency, percentage, color='y', marker='^' ,linewidth=2, label='distributed-index-cache')
axarr[1].set_title('Balanced')

axarr[2].plot(central_index_update_heavy_latency, percentage, color='b', marker='+', linewidth=2, label='central-index',)
axarr[2].plot(distributed_index_update_heavy_latency, percentage, color='r', marker='x', linewidth=2, label='distributed-index')
axarr[2].plot(distributed_index_cache_update_heavy_latency, percentage, color='y', marker='^', linewidth=2, label='distributed-index-cache')
axarr[2].set_title('Update-heavy')

for ax in axarr.flat:
    ax.set(xlabel='Query response time (ms)', ylabel='CDF (%)')

handles, labels = axarr[0].get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=3, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('latency_cdf.png')
plt.close()