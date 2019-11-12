import time

def get_bytes(t, iface='ens192'):
    with open('/sys/class/net/' + iface + '/statistics/' + t + '_bytes', 'r') as f:
        data = f.read();
    return int(data)

if __name__ == '__main__':
    (tx_prev, rx_prev) = (0, 0)

    while(True):
        tx = get_bytes('tx')
        rx = get_bytes('rx')

        if tx_prev > 0:
            tx_speed = tx - tx_prev
            print('TX: ', tx_speed/1000000, 'Mbps')

        if rx_prev > 0:
            rx_speed = rx - rx_prev
            print('RX: ', rx_speed/1000000, 'Mbps')

        time.sleep(1)

        tx_prev = tx
        rx_prev = rx
