from math import ceil, floor

def chop(l, n):
    """
    Chop l into n chunks while preserving order

    TODO: Review this because it looks way too complicated..
    """
    len_l = len(l)

    if len_l < n:
        return map(lambda i: [i], l)

    base = len_l % n
    quotient = len_l / float(n)
    c = int(ceil(quotient))
    f = int(floor(quotient))

    chunks = []

    def build(c_num, c_size, it):
        for c in xrange(c_num):
            chunk = []
            for i in xrange(c_size):
                chunk.append(l[it])
                it += 1

            chunks.append(chunk)

        return it

    it = build(base, c, 0)
    build(n - base, f, it)
    return chunks