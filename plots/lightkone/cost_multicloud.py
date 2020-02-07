# libraries
import numpy as np
import matplotlib.pyplot as plt

barWidth = 0.25

bars1 = [66.01902159, 82.85339157, 687.6625911]
bars2 = [116.7868639, 104.1446208, 82.87298137]
bars3 = [266.2098273, 232.3087784, 91.59712892]

r1 = np.arange(len(bars1))
r2 = [x + barWidth for x in r1]
r3 = [x + barWidth for x in r2]

fig, ax = plt.subplots()

ax.bar(r1, bars1, color='b', width=barWidth, edgecolor='white', label='central-index')
ax.bar(r2, bars2, color='r', width=barWidth, edgecolor='white', label='distributed-index')
ax.bar(r3, bars3, color='y', width=barWidth, edgecolor='white', label='distributed-index-cache')

plt.xlabel('Workload type')
plt.ylabel('Data transfer cost (* $ price per GB)')
plt.xticks([r + barWidth for r in range(len(bars1))], ['Query-heavy', 'Balanced', 'Update-heavy'])

handles, labels = ax.get_legend_handles_labels()
fig.legend(handles,labels=labels, loc='lower center', ncol=3, frameon=False)

fig.tight_layout(rect=[0,0.05,1,1])
plt.savefig('cost_multicloud.png')
plt.close()
