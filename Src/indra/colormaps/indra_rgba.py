import numpy as np

def gen_gradient(arr, start, stop, startVal, stopVal):
    count = stop - start
    rangeVal = stopVal - startVal
    step = rangeVal / count
    temparr = []
    if step == 0:
        ndarray = [startVal] * count
    else:
        temparr = np.arange(startVal + step, stopVal + step, step)
        ndarray = temparr.round().astype(np.uint8)

    return np.append(arr, ndarray).astype(np.uint8)


r_marks =  [ 0,            1,           5,           25,            125,           625]
r_colors = [(0, 48, 128), (0, 0, 128), (0, 128, 0), (128, 128, 0), (255, 165, 0), (128, 0, 0)]

reds = np.array([])
greens = np.array([])
blues = np.array([])

reds = np.append(reds, [0, 0]).astype(np.uint8)
greens = np.append(greens, [48, 0]).astype(np.uint8)
blues = np.append(blues, [128, 128]).astype(np.uint8)

reds = gen_gradient(reds, 1, 5, 0, 0)
greens = gen_gradient(greens, 1, 5, 0, 128)
blues = gen_gradient(blues, 1, 5, 128, 0)

reds = gen_gradient(reds, 5, 25, 0, 128)
greens = gen_gradient(greens, 5, 25, 128, 128)
blues = gen_gradient(blues, 5, 25, 0, 0)

reds = gen_gradient(reds, 25, 125, 128, 255)
greens = gen_gradient(greens, 25, 125, 128, 165)
blues = gen_gradient(blues, 25, 125, 0, 0)

reds = gen_gradient(reds, 125, 625, 128, 255)
greens = gen_gradient(greens, 125, 625, 128, 165)
blues = gen_gradient(blues, 125, 625, 0, 0)

reds = reds[0:255]
greens = greens[0:255]
blues = blues[0:255]

alpha = np.full(255, 255, dtype=np.uint8)

cmap_data = np.stack((
    reds, greens, blues, alpha
), axis=1)

np.save('indra_rgba.npy', cmap_data)
