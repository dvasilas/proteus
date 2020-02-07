import matplotlib.pyplot as plt

client_num = [1, 2, 4, 8, 16, 32, 64, 128, 256]
x = list(range(len(client_num)))

local_index_query_heavy = [1, 1, 1, 1, 1, 1, 1, 1, 1]
remote_index_query_heavy = [8.074254255, 4.458192906, 0.04410939348, 0.02175467493, 0.7349216501, 1.103756187, 1.132706888, 0.9506770041, 0.9534178106]
remote_index_cache_query_heavy = [8.09107128, 4.485082626, 0.04380880368, 0.01871174233, 0.4664436288, 0.8259291172, 0.8282160442, 0.8155285282, 0.7497195013]

plt.close('all')

f, ax = plt.subplots()

plt.setp(ax, xticks=x, xticklabels=client_num)

ax.plot(x, local_index_query_heavy, color='b', marker='+', linewidth=2, label='local-index')
ax.plot(x, remote_index_query_heavy, color='r', marker='x', linewidth=2, label='remote-index')
ax.plot(x, remote_index_cache_query_heavy, color='y', marker='^', linewidth=2, label='remote-index-cache')
ax.set_title('Query-heavy')

ax.set(xlabel='Number of client threads', ylabel='Relative freshness latency (\% of local-index)')

handles, labels = ax.get_legend_handles_labels()
f.legend(handles,labels=labels, loc='lower center', ncol=3, frameon=False)

plt.tight_layout(rect=[0,0.05,1,1])
plt.savefig('freshness_relative_clients.png')
plt.close()