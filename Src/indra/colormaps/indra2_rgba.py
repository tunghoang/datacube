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


r_marks =  [ 0,           10,           20,            50,          100,           190,           255]
r_colors = [(0, 0, 128), (0, 30, 128), (0, 128, 128), (0, 128, 0), (128, 128, 0), (255, 128, 0), (255, 0, 0)]

reds = np.array([])
greens = np.array([])
blues = np.array([])

reds = np.append(reds, [0]).astype(np.uint8)
greens = np.append(greens, [0]).astype(np.uint8)
blues = np.append(blues, [128]).astype(np.uint8)

start = 0; stop = 10
reds = gen_gradient(reds, start, stop, 0, 0)
greens = gen_gradient(greens, start, stop, 0, 30)
blues = gen_gradient(blues, start, stop, 128, 128)

start = 10; stop = 20
reds = gen_gradient(reds, start, stop, 0, 0)
greens = gen_gradient(greens, start, stop, 30, 128)
blues = gen_gradient(blues, start, stop, 128, 128)

start = 20; stop = 50
reds = gen_gradient(reds, start, stop, 0, 0)
greens = gen_gradient(greens, start, stop, 128, 128)
blues = gen_gradient(blues, start, stop, 128, 0)

start = 50; stop = 100
reds = gen_gradient(reds, start, stop, 0, 128)
greens = gen_gradient(greens, start, stop, 128, 128)
blues = gen_gradient(blues, start, stop, 0, 0)

start = 100; stop = 190
reds = gen_gradient(reds, start, stop, 128, 255)
greens = gen_gradient(greens, start, stop, 128, 128)
blues = gen_gradient(blues, start, stop, 0, 0)

start = 190; stop = 255
reds = gen_gradient(reds, start, stop, 255, 255)
greens = gen_gradient(greens, start, stop, 128, 0)
blues = gen_gradient(blues, start, stop, 0, 0)

reds = reds[0:255]
greens = greens[0:255]
blues = blues[0:255]

alpha = np.full(255, 255, dtype=np.uint8)

cmap_data = np.stack((
    reds, greens, blues, alpha
), axis=1)

np.save('indra2_rgba.npy', cmap_data)
