
def items(input):
    fh = open(input)
    d = {}
    for line in fh:
        items_info = line.split('|')
        d[int(items_info[0])] = items_info[1]

    return d


def normalize(input, item_input, output):
    fh = open(input)
    fo = open(output, 'w')

    info_items = items(item_input)
    for line in fh:
        user_id, item_id, rating, timestamp = line.split('\t')
        fo.write('|'.join([user_id, info_items[int(item_id)], rating]) + '\n')

    fo.close()
    fh.close()

normalize('u.data', 'u.item', 'ratings.csv')
