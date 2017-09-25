# Simulations

## Tests

| Test | Description                                                                                                                                                                                                        |
|---   | ---                                                                                                                                                                                                                |
|    1 | Simulations of MLFD with different values of constant k to compute the safety margin, RTT samples are from a normal distribution.                                                                                  |
|    2 | Simulations of EPFD with different values of delta to control how much to increase timeout for each false suspiciion, RTT samples are from a normal distribution.                                                  |
|    3 | Simulations of MFLD and EPFD for different probabilities of message loss. RTT samples are from a normal distribution                                                                                               |
|    4 | Simulations of MLFD and EPFD with RTTs sampled from different distributions including normal, weibull, exponential. As well as a test where RTTs are not correlated with node features such as geographic location |

## How to run

`./run.sh` inside each directory. 