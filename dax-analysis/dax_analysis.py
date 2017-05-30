import argparse
import sys
import os
import errno

import time

import xml.etree.ElementTree as ET

import matplotlib.pyplot as plt

import numpy as np
import random
import colorsys

from sklearn.cluster import MeanShift, estimate_bandwidth

DIST = 8

NAME    = 0
COLOR   = 1
X_COORD = 2
Y_COORD = 3

def check_path(path):

	# Check if the directory specified by path exists and create if not
	# path : path to check
	
	try:
		os.makedirs(path)
	
	except OSError as exc:
		
		if exc.errno == errno.EEXIST and os.path.isdir(path):
			pass
	
		else: raise

class DAG(object):

	def __init__(self):
		self.nodes = {}

	def add_node(self, name, parents):

		if not name in self.nodes:
			self.nodes[name] = parents

		else:
			self.nodes[name].append(parents)

		for p in parents:
			if not p in self.nodes:
				self.nodes[p] = []

	def clear(self):
		self.nodes = {}

	def draw(self, node_dict, savename="default", save=True):
		
		# Coordinates where the next node is to be drawn
		cur_x = 0
		cur_y = 0
		min_x = 0
		max_x = 0

		fig, ax = plt.subplots()

		edges = self.get_edges()
		leafs = self.find_stumps()

		while(leafs):

			# Calculate the position of the left most node in this row
			cur_x = -(((len(leafs)-1) * DIST)/2)

			if cur_x < min_x:

				# Update x range if neccessary
				min_x = cur_x

			for node in leafs:

				# Add the coordinates of current node to the node dictionary
				node_dict[node].append(cur_x)
				node_dict[node].append(cur_y)

				# Draw a circle for the current node
				circ = plt.Circle((cur_x, cur_y), radius=2, color=str(node_dict[node][COLOR]), fill=True)
				ax.add_artist(circ)

				# Display the name of the node under the node	
				ax.annotate(node_dict[node][NAME][1:4], xy=(cur_x, cur_y), fontsize=6, va='center', ha='center') 

				# Update X coordinate for next node
				cur_x += DIST

#			print(node_dict["ID00002"])

			if cur_x > max_x:

				# Update the x range if neccessary
				max_x = cur_x

			# Update the y coordinate for the next nodes
			cur_y += DIST

			# Get nodes of the next row
			leafs= self.find_stumps()

		# Draw Edges
		for edge in edges:

			x1 = node_dict[edge[0]][X_COORD]
			x2 = node_dict[edge[1]][X_COORD]
			y1 = node_dict[edge[0]][Y_COORD]+2
			y2 = node_dict[edge[1]][Y_COORD]-2
			ax.plot((x1, x2), (y1, y2), 'k-', linewidth=.1)

		# Keep aspect ratio and remove axis lables
		ax.set_aspect('equal', adjustable='box')
		ax.axis('off')

		# Set axis range
		#plt.axis([min_x-DIST,max_x+DIST,-DIST,cur_y+DIST])
		plt.axis([max(-50, min_x-DIST), min(50, max_x+DIST), -DIST, cur_y+DIST])

		if save:
			#plt.savefig(savename + ".png", dpi=900, format='png', bbox_inches='tight', pad_inches=0)
			plt.savefig(savename + ".png", dpi=900, format='png', pad_inches=0)

		else:
			plt.show()

		plt.close()

	def find_stumps(self):

		nodes = []

		# Find all stumps
		for node in self.nodes:
			if self.nodes[node] == []:
				nodes.append(node)

		# Remove stumps
		for node in nodes:
			self.nodes.pop(node)

		# Remove stumps from the lists of parents
		for node in self.nodes:
			for stump in nodes:
				if stump in self.nodes[node]:
					self.nodes[node].remove(stump)

		return nodes

	def get_edges(self):
		
		edges = []

		for node in self.nodes:
			for parent in self.nodes[node]:
				edges.append((parent, node))

		return edges

def plot_task_inforamtion(task_data, savename="default.png", save=True):

	data_stats = {}

	for task, data in task_data.items():

		if len(data) < 5:
			continue
	
		fig, ax = plt.subplots()

		data = np.array(data)

		# Draw all datapoints
		for point in data:
			ax.plot(point[0], point[2], 'ko')

		# Seperate x and y coordinates
		x = data[:,0]
		y = data[:,2]

		# Perform meanshift clustering on the x coordinates
		ms = MeanShift()
		ms.fit(x.reshape(-1,1))

		labels = ms.labels_

		# A variable to remember if any clusters were used
		numclusters = 0

		for k in range(len(np.unique(labels))):
			
			# Get the indixes of the points belonging to the current cluster
			members = labels == k

			# If the cluster is to small ignore it
			if len(x[members]) < 3:
				continue

			numclusters += 1
			
			# Calculate mean and standard deviation in x and y direction
			mean_x = np.mean(x[members])
			mean_y = np.mean(y[members])
			std_x  = np.std(x[members])
			std_y  = np.std(y[members])

			# Remember this information for later to draw the global information
			if not task in data_stats:
				data_stats[task] = [[mean_x, mean_y, std_x, std_y]]

			else:
				data_stats[task].append([mean_x, mean_y, std_x, std_y])

			# Draw the mean
			ax.plot(mean_x, mean_y, 'bo')

			# Lines to indicate standard deviation
#			ax.axvline(mean_x+std_x, color='k', linestyle='--')
#			ax.axvline(mean_x-std_x, color='k', linestyle='--')
#			ax.axhline(mean_y+std_y, color='k', linestyle='--')
#			ax.axhline(mean_y-std_y, color='k', linestyle='--')

		# If no cluster has been used, use all datapoints as one cluster
		if numclusters == 0:

			# Calculate mean and standard deviation in x and y direction
			mean_x = np.mean(x)
			mean_y = np.mean(y)
			std_x  = np.std(x)
			std_y  = np.std(y)

			# Remember this information for later to draw the global information
			if not task in data_stats:
				data_stats[task] = [[mean_x, mean_y, std_x, std_y]]

			else:
				data_stats[task].append([mean_x, mean_y, std_x, std_y])

			# Draw the mean
			ax.plot(mean_x, mean_y, 'bo')

		if save:
			plt.savefig(savename + "_" + task + ".png", dpi=900, format='png', pad_inches=0)

		else:
			plt.show()

		plt.close()

	return data_stats

def plot_global_task_information(task_data, savename="default.png", save=True):

	for task, data in task_data.items():

#		x = []
#		y = []

#		xmin = float("inf")
#		xmax = 0

#		yavrg = 0
	
		fig, ax = plt.subplots()

		for point in data:

			#ax.plot([point[0]], [point[2]], marker='o', color=point[3], alpha=.5)
			ax.plot([float(point[0])], [float(point[1])], marker='o', color=point[4], alpha=.5)

#			if xmin > int(point[0]):
#				xmin = int(point[0])

#			if xmax < int(point[0]):
#				xmax = int(point[0])

#			yavrg += float(point[2])

#			x.append(point[0])
#			y.append(point[2])

		plt.title(task[1:])
		plt.ylabel("time [s]")
		plt.xlabel("input [B]")

#		delta = (xmax-xmin)/100
#		yavrg /= len(y)

#		for i in range(len(x)):
#			for j in range(len(x)):
#				if abs(x[i]-x[j]) < delta:
#					if abs(float(y[i])-float(y[j])) < yavrg/10:
#						plt.axvline((abs(i-j)/2) - delta)
#						plt.axvline((abs(i-j)/2) + delta)

		if save:
			plt.savefig(savename + task + ".png", dpi=900, format='png', pad_inches=0)

		else:
			plt.show()

		plt.close()

def main(argv):

	parser = argparse.ArgumentParser()
	parser.add_argument("-i", "--inpath", default="../data/SyntheticWorkflows/", help="path to the input directory")
	parser.add_argument("-o", "--outpath", default="../data/results/", help="path to the output directory")
	args = parser.parse_args()

	colors = {}

	# Global information about tasks
	# name: [mean_insize, mean_time, std_insize, std_time, color]
	global_task_data = {}

	graph_colors = []
	graph = DAG()

	for path in os.walk(args.inpath):

#		print("\n" + path[0])

		for file in path[2]:

			if file[-4:] != ".dax":
				continue

			dag_color = ("#%02X%02X%02X" % (random.randint(0,255),random.randint(0,255),random.randint(0,255)))

			if not len(graph_colors) == 0:
				while dag_color in graph_colors:
					dag_color = ("#%02X%02X%02X" % (random.randint(0,255),random.randint(0,255),random.randint(0,255)))

			graph_colors.append(dag_color)

			# Progress output
#			sys.stdout.write("   " + file + " "*(40 - len(file)) + "\r")
#			sys.stdout.flush()

			f       = open(path[0] + "/" + file, "r")
			content = f.read()

			# Parse the xml content of the file
			root = ET.fromstring(content)

			# Get the length of the prefix, thus pos is the position of the first element after the prefix
			pos = (root.tag).find("}")+1
			
			# Clean DAG
			graph.clear()

			# Store information about the nodes in the DAG (id : [name, color, x, y])
			node_dict = {}

			# Store data about the tasks in the DAG (name : [[(input size, output size, exec time)]])
			task_data = {}

			for child in root:

				# Handle elements, that do not describe parts of the DAG
				if not child.tag[pos:] == "child":
					
					# Fill the node dictionary
					if child.tag[pos:] == "job":
						
						# Determine the color of the node
						if not child.attrib["name"] in colors:

							# Find a new color
							h,s,l = random.random(), 0.5 + random.random()/2.0, 0.4 + random.random()/5.0
							r,g,b = [int(256*i) for i in colorsys.hls_to_rgb(h,l,s)]
							colors[child.attrib["name"]] = ("#%02X%02X%02X" % (r,g,b))
							#colors[child.attrib["name"]] = ("#%02X%02X%02X" % (random.randint(0,255),random.randint(0,255),random.randint(0,255)))
						
						# Assign color to the node
						node_dict[child.attrib["id"]] = [child.attrib["name"], colors[child.attrib["name"]]]

						# Compute the otal input and output data size
						data_in  = 0
						data_out = 0
						
						for use in child:

							if use.attrib["link"] == "input":
								data_in += int(use.attrib["size"])

							elif use.attrib["link"] == "output":
								data_out += int(use.attrib["size"])

						# Store information in task_data
						if not child.attrib["name"] in task_data:
							task_data[child.attrib["name"]] = [[data_in, data_out, float(child.attrib["runtime"])]]

						else:
							task_data[child.attrib["name"]].append([data_in, data_out, float(child.attrib["runtime"])])

						# Store task information in global dictionary
#						if not child.attrib["name"] in global_task_data:
#							global_task_data[child.attrib["name"]] = [(data_in, data_out, child.attrib["runtime"], dag_color)]

#						else:
#							global_task_data[child.attrib["name"]].append((data_in, data_out, child.attrib["runtime"], dag_color))

					continue

				parents = []

				# Get all parents of this node
				for parent in child:
					parents.append(parent.attrib["ref"])
				
				# Add this node to the graph
				graph.add_node(child.attrib["ref"], parents)
			
			# Once the DAG is fully constructed draw it
			outpath = args.outpath + (path[0][len(args.inpath):] + "/" + file[:-4]).replace(".", "_")
			check_path(outpath)
			graph.draw(node_dict, outpath + "/" + file[:-4])

			# Draw the plots for individual tasks
			stats = plot_task_inforamtion(task_data, outpath + "/" + file[:-4])

			for task, data in stats.items():

				for point in data: 

					point.append(colors[task])
					
					if not task in global_task_data:
						global_task_data[task] = [point]
					else:
						global_task_data[task].append(point)

#			sys.stdout.write("")
#			sys.stdout.flush()

	#print(args.outpath + "global", graph_colors, global_task_data)
	check_path(args.outpath + "global")
	plot_global_task_information(global_task_data, args.outpath + "global/")

	# -----------------------------------------------------------------------------------------------------------------------
	# Stuff for testing

	# -----------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
	main(sys.argv)