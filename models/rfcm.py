import time
from multiprocessing import Pool, Process
import multiprocessing
from turtle import distance

import numpy as np
import sklearn.base as skb
import sklearn.utils.validation as skv
from numba import njit
from pyts.metrics import dtw


@njit()
def distance(x, y):
    return abs(x - y)

class RFCM(skb.ClassifierMixin, skb.BaseEstimator):
    r"""
    Revised Fuzzy C-Means clustering algorithm.
    """

    def __init__(self, n_clusters=2, epsilon=0.5, expo=2, max_iter=100, alpha=1, p=2, random_state=100, n_jobs=1):
        self.n_clusters = n_clusters
        self.epsilon = epsilon
        self.expo = expo
        self.max_iter = max_iter
        self.alpha = alpha
        self.p = p
        self.random_state = random_state
        self.n_jobs = n_jobs if 0 < n_jobs < multiprocessing.cpu_count() else multiprocessing.cpu_count()

    def init_memval(self, n_clusters, n_data):
        U = np.random.random((n_clusters, n_data))
        val = sum(U)
        U = np.divide(U, np.dot(np.ones((n_clusters, 1)),
                      np.reshape(val, (1, n_data))))
        return U

    def do_one_calc_dtw(self, line, center_node):
        dist_feature = []
        for index in range(len(line)):
            cost = dtw(line[index].get_data(), center_node[index].get_data(), dist=distance)
            cost /= len(line[index]) + len(center_node[index])
            dist_feature.append(cost)
        return np.linalg.norm(np.array(dist_feature))

    def calc_dtw(self, data, center):
        dist = []
        # pool = Pool(processes=self.n_jobs)
        for center_node in center:
            # dist_center_node = []
            # for line in data:
            #     dist_feature = []
            #     for index in range(len(line)):
            #         cost = dtw(line[index].get_data(), center_node[index].get_data(), dist=distance)
            #         cost /= len(line[index]) + len(center_node[index])
            #         dist_feature.append(cost)
            #     dist_center_node.append(np.linalg.norm(np.array(dist_feature)))
            with Pool(processes=self.n_jobs) as pool:
                dist_center_node = pool.starmap(self.do_one_calc_dtw, [(line, center_node) for line in data])
            dist.append(dist_center_node)
        return np.array(dist)
    
    def center_diff(self, center, center_old):
        diff = []
        for i in range(len(center)):
            feature = []
            for j in range(len(center[i])):
                feature.append(center[i][j].get_data() - center_old[i][j].get_data())
            diff.append(np.array(feature))
        return np.array(diff)

    def _size_insensitive_rfcm(self, data, n_clusters, epsilon, expo, max_iter, p):
        """
        Size insensitive object function.

        Args:
            data (array-like, dataframe): Dataset
            n_clusters (int): The number of the clusters
            epsilon (float): The threshold of the convergence
            expo (float): The degree of the fuzziness
            max_iter (int): The maximum number of iterations
            p (int): The fuzziness parameter
        """
        print("----- Start size insensitive rfcm -----")
        np.random.seed(0)
        n_data = data.shape[0]  # Number of data points
        # Initialize the partition matrix
        U = self.init_memval(n_clusters, n_data)
        t_iter = time.time()
        for i in range(max_iter):
            # X[j] is the jth data point, U[i, j] is the membership degree
            mf = np.power(U, expo)
            # The center of the cluster, v_i in the paper
            center = np.divide(
                np.dot(mf, data), (np.ones((data.shape[1], 1))*sum(mf.T)).T + 0.00001)

            # j belongs to A_i,
            # A_i is the set of data points that belong to cluster i
            membership = np.equal(U, U.max(axis=0))
            membership_size = np.sum(membership + np.divide(np.multiply(
                membership, U), np.power(n_data, p)), axis=1)   # According to the paper
            relative_size = np.divide(membership_size, n_data).reshape(
                n_clusters, 1)      # The relative size of the cluster, S_i in the paper

            index_array = np.argmax(U, axis=0)
            interaction_reduction = np.subtract(
                1, np.take_along_axis(relative_size.T, np.expand_dims(index_array, axis=-1), axis=-1)).T  # The interaction reduction, Rho_j in the paper

            # distance part of the U update equation
            dist_part = self.calc_dtw(data, center) ** (-1 / (expo - 1))

            # coefficient part of the U update equation
            coef_part = np.power(
                1 + np.multiply(membership, np.divide(1, np.power(n_data, p + 1))), (-1 / (expo - 1)))

            # Update the partition matrix
            U = interaction_reduction * np.einsum("ijk->ik", np.divide(
                coef_part[:, None, :] * dist_part, dist_part[:, None, :] * coef_part)) ** -1

            # Check the convergence
            if i > 0:
                d = np.linalg.norm(self.center_diff(center,  center_old))
                print("->", d, end=" ")
                if d < epsilon:
                    print("Convergence in {} iterations, difference is {}".format(i, d))
                    break

            center_old = center

        print()
        return U, center

    def _exp_func(self, diff, omega):
        return 1 - np.exp(-diff / omega)

    def _exp_derivative_func(self, diff, omega):
        return (1 / omega) * np.exp(-diff / omega)

    def _noise_resistant_rfcm(self, data, n_clusters, epsilon, expo, alpha, max_iter, p):
        U, center = self._size_insensitive_rfcm(
            data, n_clusters, epsilon, expo, max_iter, p)

        print("----- Start noise resistant rfcm -----")
        for i in range(max_iter):
            # distance part of the U update equation
            diff = self.calc_dtw(data, center).T
            temp = diff ** (1 / (expo - 1))
            denominator_ = temp.reshape((data.shape[0], 1, -1)).repeat(
                temp.shape[-1], axis=1
            )
            denominator_ = temp[:, :, np.newaxis] / (denominator_ + 0.00001)
            # The distance between the data point and the cluster center, S_ij in the paper
            dist = (1 / denominator_.sum(2)) ** expo

            # The omega parameter, Omega_i in the paper
            omega = sum(dist * diff) / (alpha * sum(dist))

            # distance part of the U update equation
            dist_part = (self._exp_func(diff, omega) ** (-1 / (expo - 1))).T

            # coefficient part of the U update equation
            coef_part = np.ones_like(U)  # \phi = 1 and it's derivative is 0

            # Update the partition matrix
            numerator_part = np.dot(np.ones((n_clusters, 1)), np.reshape(
                sum(coef_part), (1, coef_part.shape[1]))) * dist_part
            denominator_part = np.dot(np.ones((n_clusters, 1)), np.reshape(
                sum(dist_part), (1, dist_part.shape[1]))) * coef_part
            # The U update equation
            U = 1 * np.divide(numerator_part, denominator_part) ** -1

            # X[j] is the jth data point, U[i, j] is the membership degree
            mf = np.power(U, expo)
            center_old = center
            center = np.divide(
                np.dot(mf * self._exp_derivative_func(diff, omega).T, data),
                (np.ones((data.shape[1], 1))*sum(mf.T * self._exp_derivative_func(diff, omega))).T + 0.00001)    # The center of the cluster, v_i in the paper

            # Check the convergence
            if i > 0:
                d = np.linalg.norm(self.center_diff(center,  center_old))
                print("->", d, end=" ")
                if d < epsilon:
                    print("Convergence in {} iterations, difference is {}".format(i, d))
                    break
        
        print()
        return U, center

    def fit(self, data, y=None, sample_weight=None):
        # data = self._validate_data(data)
        data = self._transform(data)

        if not self.epsilon > 0.0:
            raise ValueError("epsilon must be positive.")

        if sample_weight is not None:
            sample_weight = skv._check_sample_weight(sample_weight, data)

        U, center = self._noise_resistant_rfcm(
            data, self.n_clusters, self.epsilon, self.expo, self.alpha, self.max_iter, self.p)

        self.cluster_centers_ = center
        self._n_cluster_out = self.cluster_centers_.shape[0]
        self.labels_ = np.argmax(U, axis=0)

        return self

    def predict(self, X):
        pass

    def fit_predict(self, X, y=None, sample_weight=None):
        return self.fit(X, y, sample_weight).labels_
    
    def _transform(self, data):
        outer = []
        for i in range(len(data)):
            inner = []
            for j in range(len(data[i])):
                inner.append(TimeSeries(data[i][j]))
            outer.append(inner)
        return np.array(outer)
    
class TimeSeries:
    def __init__(self, data: list):
        self.data = np.array(data)
    
    def __add__(self, other):
        if isinstance(other, TimeSeries):
            return TimeSeries(self.data + other.data)
        elif isinstance(other, int):
            return TimeSeries(self.data + other)
        elif isinstance(other, float):
            return TimeSeries(self.data + other)
        elif isinstance(other, np.ndarray):
            return TimeSeries(self.data + other)
        
    def __radd__(self, other):
        return self + other
        
    def __sub__(self, other):
        if isinstance(other, TimeSeries):
            return TimeSeries(self.data - other.data)
        elif isinstance(other, int):
            return TimeSeries(self.data - other)
        elif isinstance(other, float):
            return TimeSeries(self.data - other)
        elif isinstance(other, np.ndarray):
            return TimeSeries(self.data - other)
    
    def __rsub__(self, other):
        return -self + other
    
    def __mul__(self, other):
        if isinstance(other, TimeSeries):
            return TimeSeries(self.data * other.data)
        elif isinstance(other, int):
            return TimeSeries(self.data * other)
        elif isinstance(other, float):
            return TimeSeries(self.data * other)
        elif isinstance(other, np.ndarray):
            return TimeSeries(self.data * other)
    
    def __rmul__(self, other):
        return self * other
        
    def __truediv__(self, other):
        if isinstance(other, TimeSeries):
            return TimeSeries(self.data / other.data)
        elif isinstance(other, int):
            return TimeSeries(self.data / other)
        elif isinstance(other, float):
            return TimeSeries(self.data / other)
        elif isinstance(other, np.ndarray):
            return TimeSeries(self.data / other)
    
    def __floordiv__(self, other):
        if isinstance(other, TimeSeries):
            return TimeSeries(self.data // other.data)
        elif isinstance(other, int):
            return TimeSeries(self.data // other)
        elif isinstance(other, float):
            return TimeSeries(self.data // other)
        elif isinstance(other, np.ndarray):
            return TimeSeries(self.data // other)
        
    def __pow__(self, other):
        if isinstance(other, TimeSeries):
            return TimeSeries(self.data ** other.data)
        elif isinstance(other, int):
            return TimeSeries(self.data ** other)
        elif isinstance(other, float):
            return TimeSeries(self.data ** other)
        elif isinstance(other, np.ndarray):
            return TimeSeries(self.data ** other)
    
    def __neg__(self):
        return TimeSeries(-self.data)
    
    def __repr__(self):
        return str(self.data)
    
    def __len__(self):
        return len(self.data)
    
    def append(self, other):
        self.data = np.append(self.data, other)
        return self
    
    def extend(self, other):
        self.data = np.append(self.data, other.data)
        return self
    
    def size(self):
        return len(self.data)
    
    def get_data(self):
        return self.data