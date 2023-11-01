import numpy as np
from enum import Enum, auto
from scipy.stats import gamma, lognorm, norm, weibull_min

class DistributionGenerator:
    class DistributionFamily(Enum):
        LOGNORMAL = auto()
        GAMMA = auto()
        WEIBULL = auto()
        NORMAL = auto()

    def __init__(self, dist: str, scale: float, shape: float, a: float = None, b: float = None, c: float = None):
        self.dist = dist
        self.scale = scale
        self.shape = shape
        self.scale_prior = scale
        self.shape_prior = shape
        self.likelihood_prior = c if c is not None else 0.0
        self.samples = self.get_distribution_samples(scale, shape)
        self.cumulativeSamples = np.cumsum(self.samples)
        self.cursor = 0
        self.SAMPLE_SIZE = 1500  # DistributionGenerator will automatically increase the size

    def get_samples(self) -> np.ndarray:
        return self.samples

    def get_cumulative_samples(self) -> np.ndarray:
        return self.cumulativeSamples

    def extend_samples(self):
        new_samples = self.get_distribution_samples(self.scale, self.shape)
        self.samples = np.concatenate((self.samples, new_samples))
        self.cumulativeSamples = np.cumsum(self.samples)

    def get_pkem_mean(self) -> float:
        return self.shape_prior / self.scale_prior

    def get_mean(self) -> float:
        return np.mean(self.samples[:self.cursor])

    def get_likelihood_prior(self) -> float:
        return self.likelihood_prior

    def get_mle_mean(self) -> float:
        a, b = self.shape_prior, self.scale_prior
        sum_val = 0.0

        for i in range(self.cursor):
            if self.dist == 'GAMMA':
                sum_val += self.samples[i]
            elif self.dist == 'WEIBULL':
                sum_val += self.samples[i] ** self.likelihood_prior

        if self.dist == 'GAMMA':
            result = (b + sum_val) / (a + self.cursor * self.likelihood_prior - 1)
        elif self.dist == 'WEIBULL':
            result = (b + sum_val) / (a + self.cursor + 1)
        else:
            result = 0.0

        return result

    def vary_distribution(self, scale: float, shape: float):
        self.scale = scale
        self.shape = shape
        self.samples = self.get_distribution_samples(scale, shape)
        self.cumulativeSamples = np.cumsum(self.samples)

    def concat(self, first: np.ndarray, second: np.ndarray) -> np.ndarray:
        return np.concatenate((first, second))

    def get_next_sample(self) -> float:
        while self.cursor >= len(self.samples):
            new_samples = self.get_distribution_samples(self.scale, self.shape)
            self.samples = self.concat(self.samples, new_samples)
            self.cumulativeSamples = np.cumsum(self.samples)

        delay = self.samples[self.cursor]
        self.cursor += 1
        return delay

    def get_distribution_samples(self, scale: float, shape: float) -> np.ndarray:
        if self.dist == 'LOGNORMAL':
            return lognorm.rvs(s=shape, scale=np.exp(scale), size=self.SAMPLE_SIZE)
        elif self.dist == 'GAMMA':
            return gamma.rvs(a=shape, scale=scale, size=self.SAMPLE_SIZE)
        elif self.dist == 'WEIBULL':
            return weibull_min.rvs(c=shape, scale=scale, size=self.SAMPLE_SIZE)
        elif self.dist == 'NORMAL':
            return norm.rvs(loc=scale, scale=shape, size=self.SAMPLE_SIZE)
        else:
            return np.array([])

    def get_scale(self) -> float:
        return self.scale

    def get_shape(self) -> float:
        return self.shape