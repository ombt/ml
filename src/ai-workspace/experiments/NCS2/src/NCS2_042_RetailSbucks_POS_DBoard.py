import matplotlib.pylab as plt
import matplotlib.animation as animation
import csv
import numpy as np
import networkx as nx
from dask.array.random import randint
from itertools import count

np.random.seed(0)  # seed for reproducibility

fig = plt.figure("Real Time Graph : MOST SOLD COMBINATIONS ")
plt.style.use('seaborn')

# Key will be :  <prod_1 label> + <prod_2.label> where prod_1 < prod_2
# Value = weight
# Limit this to the top 100
prod_weights = {}

pershiable_list = [16,1,12,4,3,21,12,20]
mappingfile = open('/home/s/ai-workspace/experiments/NCS2/data/instakart/s1_products.csv','r')
names_mapping = csv.reader(mappingfile, delimiter=',')

node_labels = {}
organic_groups = {}
perishable_mapping = {}
next(names_mapping)  # Skip the header
for rows in names_mapping:
    prod_id = int(rows[0])
    node_labels[prod_id] = rows[1]
    organic_groups[prod_id] = rows[2]
    
    chk = int(rows[0])
    if chk in pershiable_list:
        perishable_mapping[prod_id] = 1
    else:
        perishable_mapping[prod_id] = 0

mappingfile.close()


csvfile = open('/home/s/ai-workspace/experiments/NCS2/data/instakart/order_products__prior.csv','r')
order_products =csv.reader(csvfile, delimiter=',')
# Create the meta table with weights
# Simple to beginn with.
# If a product is in the same order then add weight to the edge.
# We will start with a weight of 0.1
prev_order_num = -1
prev_prod_id = -1
item = -1
prod_1 = {}
edge_connects = []
G=nx.Graph()

show_organic = True

next(order_products)

line_ctr = 0

          
def animate(i):
    
    global prod_weights
    global order_products
    global organic_groups
    global perishable_mapping
    
    global line_ctr
    global prev_order_num
    global prev_prod_id
    global item
    global show_organic

    global prod_1
    global edge_connects
    global G
        
    sleep_trigger = 500
    
    read_ctr = 0
    for row in order_products:
        
        if read_ctr < line_ctr:
            read_ctr += 1
            continue
        
        line_ctr += 1
        
        curr_order_num = int(row[0])
        curr_prod_id = int(row[1])
        item = int(row[2])
        
        #print("Values are {}, {},{},{}".format(line_ctr, curr_order_num, curr_prod_id, item))
        
        if item == 1:
            # This is the first item in the order.
            # Store the values and continue
            prev_order_num = curr_order_num
            prev_prod_id = int(row[1])
            continue
        else:
            # Item number > 1
            if curr_order_num == prev_order_num:
                
                min_key = min(prev_prod_id, curr_prod_id)
                max_key = max(prev_prod_id, curr_prod_id)
                key = str(min_key)+"-"+str(max_key)
                
                if key in prod_weights.keys():
                    prod_weights[key] += 1
                else:
                    prod_weights[key] = 1
                
                
        if sleep_trigger > 0:
            sleep_trigger -= 1
        else:    
            break
    
    # Sleep 2 secs
    sorted_by_value = []
    sorted_by_value = sorted(prod_weights.items(), key=lambda kv: kv[1])
    sorted_by_value.reverse()
    
    # Set max weight.
    max_weight = int(sorted_by_value[0][1]*0.6)
    print("Max Weight is {}".format(max_weight))
    
    sleep_trigger = int(randint(10,50))
    # Delete the number we expect to add.
    print("\nIncoming Transactions {} Stored Data set {}\n".format(sleep_trigger, len(sorted_by_value)))
    
    deleted_keys = sorted_by_value[(500-len(sorted_by_value)):]
        
    fig.clear()
        
    #print("\nDeleted Keys <{}>\n".format(deleted_keys))
    for k in range(len(deleted_keys)):
        kv = deleted_keys[k][0]
        del prod_weights[kv]

    # Focud only on the top 200
    prod_1.clear()
    edge_connects.clear()
    for x in range(min(20, len(sorted_by_value))):

        key = sorted_by_value[x][0]
        if key in prod_weights.keys():
            
            key1 = int(key.split('-')[0])
            key2 = int(key.split('-')[1])
            
            prod_1[node_labels[key1]] = {'organic':organic_groups[key1],
                                 'perishable': perishable_mapping[key1]
                               }
            
            prod_1[node_labels[key2]] = {'organic':organic_groups[key2],
                         'perishable': perishable_mapping[key2]
                       }
    
            edge_connects.append([node_labels[key1], node_labels[key2], prod_weights[key]])
    
    G.clear() 
    #for x in range(300):
    G.add_nodes_from(prod_1)
    nx.set_node_attributes(G, prod_1)
    
    #print(G.nodes(data=True))
    
    #G.add_edges_from(edge_connects)
    G.add_weighted_edges_from(edge_connects, weight='weight')
    
    if G:
        e_cen = nx.eigenvector_centrality(G, max_iter=10000)
        sorted_set = sorted(('{:0.2f}'.format(c),v) for v, c in e_cen.items())
        sorted_set.reverse()
        print("(Indirect Influencers: Eigen Vector Centrality Set  \n{}".format(sorted_set[:20]))
    
        d_cen = nx.load_centrality(G)
        sorted_set = sorted(('{:0.2f}'.format(c), v) for v, c in d_cen.items())
        sorted_set.reverse()
        print("Direct Influencers; Degree Centrality Set  \n{}".format(sorted_set[:20]))
    
    search_val = 'perishable'
    
    if show_organic:
        search_val = 'organic'
    
    groups = set(nx.get_node_attributes(G,search_val).values())
    
    mapping = dict(zip(sorted(groups),count()))
    
    nodes = G.nodes()
    
    colors = [mapping[G.node[n][search_val]] for n in nodes]
    
    cmap = plt.cm.winter
    if show_organic:
        cmap = plt.cm.cool
           
    # drawing nodes and edges separately so we can capture collection for colobar
    #pos = nx.spring_layout(G)
    pos = nx.kamada_kawai_layout(G, dist=None, pos=None, dim=2)
    
    # Do not use.
    #pos = nx.random_layout(G, dim=2)
    #pos = nx.shell_layout(G, dim=2)
    
    nc = nx.draw_networkx_nodes(G, pos, node_color=colors, node_size=100, cmap=cmap)
    
    elarge = [(u, v) for (u, v, d) in G.edges(data=True) if d['weight'] > max_weight]
    esmall = [(u, v) for (u, v, d) in G.edges(data=True) if d['weight'] <= max_weight]
    
    # edges
    nx.draw_networkx_edges(G, pos, edgelist=elarge, width=2, alpha=0.4)
    nx.draw_networkx_edges(G, pos, edgelist=esmall, width=1, alpha=0.2, edge_color='b', style='dashed')
    
    # ec = nx.draw_networkx_edges(G, pos, alpha=0.2)
    labels = nx.draw_networkx_labels(G, pos)
    
    sorted_by_value = sorted_by_value[:20] 
     
    print(sorted_by_value)
    print("\n")        
    plt.axes().get_xaxis().set_visible('False')
    plt.axes().get_yaxis().set_visible('False')
    # plt.colorbar(nc)
    plt.axis('off')
    fig.tight_layout()

       
ani1 = animation.FuncAnimation(fig, animate, interval=1000)
plt.show()

csvfile.close()

