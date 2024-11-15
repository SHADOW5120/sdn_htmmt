from floodlight.controller import app_manager
from floodlight.lib.packet import ethernet
from floodlight.lib.packet import ipv4
from collections import defaultdict

class LeastConnectionsLoadBalancer(app_manager.RyuApp):
    def __init__(self, *args, **kwargs):
        super(LeastConnectionsLoadBalancer, self).__init__(*args, **kwargs)
        self.server_connections = defaultdict(int)  # Lưu trữ số lượng kết nối đến các server
        self.servers = ["10.0.0.1", "10.0.0.2"]  # Địa chỉ IP của các server

    def _handle_PacketIn(self, ev):
        pkt = ev.msg.data
        eth_pkt = ethernet.ethernet(pkt)
        
        if isinstance(eth_pkt.payload, ipv4.ipv4):
            ip_pkt = eth_pkt.payload
            src_ip = ip_pkt.src
            dst_ip = ip_pkt.dst
            
            # Kiểm tra nếu địa chỉ đích là một trong các server
            if dst_ip in self.servers:
                # Cập nhật số lượng kết nối đến server
                self.server_connections[dst_ip] += 1
                # Tìm server có ít kết nối nhất
                least_conn_server = min(self.server_connections, key=self.server_connections.get)
                self.send_packet(ev, least_conn_server)
            else:
                # Nếu không phải server, gửi lại gói tin như bình thường
                self.send_packet(ev, dst_ip)

    def send_packet(self, ev, server_ip):
        # Tìm địa chỉ MAC của server và gửi gói tin
        mac = self.get_mac_address(server_ip)
        self.controller.send_msg(ev.msg)

    def get_mac_address(self, ip):
        # Giả sử các server có địa chỉ MAC tương ứng với IP
        return "00:00:00:00:00:01" if ip == "10.0.0.1" else "00:00:00:00:00:02"

# sudo mn --topo=tree,depth=2,fanout=2 --controller=remote,ip=127.0.0.1,port=6653

# app = new LeastConnectionsLoadBalancer();
