class Trie(object):
    class Node:
        def __init__(self):
            self.val = 0
            self.trans = {}

    def __init__(self):
        self.root = Trie.Node()

    def add(self, word, value=1):
        curr_node = self.root
        for ch in word:
            try:
                curr_node = curr_node.trans[ch]
            except:
                curr_node.trans[ch] = Trie.Node()
                curr_node = curr_node.trans[ch]

        curr_node.val = value

    def walk(self, trienode, ch):
        if ch in trienode.trans:
            trienode = trienode.trans[ch]
            return trienode, trienode.val
        else:
            return None, 0

    def match_prefix(self, word):
        l = ret_l = ret_v = 0
        curr_node = self.root

        for ch in word:
            curr_node, val = self.walk(curr_node, ch)
            if not curr_node:
                break

            l += 1
            if val:
                ret_l, ret_v = l, val

        return ret_v, ret_l