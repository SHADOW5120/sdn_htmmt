import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.protocol.OFPort;
import org.projectfloodlight.openflow.util.HexString;

import net.floodlightcontroller.core.*;
import net.floodlightcontroller.packet.*;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightModuleContext;
import net.floodlightcontroller.core.module.IFloodlightService;

import java.util.*;

public class LeastConnectionsLoadBalancer implements IFloodlightModule, IOFMessageListener {
    private IFloodlightProviderService floodlightProvider;
    private Map<String, Integer> serverConnections; // Lưu số lượng kết nối đến mỗi server
    private List<String> servers; // Danh sách địa chỉ IP của server

    @Override
    public String getName() {
        return "LeastConnectionsLoadBalancer";
    }

    @Override
    public boolean isCallbackOrderingPrereq(OFType type, String name) {
        return false;
    }

    @Override
    public boolean isCallbackOrderingPostreq(OFType type, String name) {
        return false;
    }

    @Override
    public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
        if (msg.getType() == OFType.PACKET_IN) {
            OFPacketIn packetIn = (OFPacketIn) msg;
            Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
            
            if (eth.getPayload() instanceof IPv4) {
                IPv4 ipPacket = (IPv4) eth.getPayload();
                IPv4Address srcIp = ipPacket.getSourceAddress();
                IPv4Address dstIp = ipPacket.getDestinationAddress();
                
                // Kiểm tra nếu địa chỉ đích là server
                if (servers.contains(dstIp.toString())) {
                    serverConnections.put(dstIp.toString(), serverConnections.getOrDefault(dstIp.toString(), 0) + 1);
                    
                    // Tìm server có ít kết nối nhất
                    String leastConnServer = findLeastConnectionsServer();
                    sendPacket(sw, cntx, leastConnServer);
                } else {
                    // Nếu không phải server, gửi gói tin như bình thường
                    sendPacket(sw, cntx, dstIp.toString());
                }
            }
        }
        return Command.CONTINUE;
    }

    private String findLeastConnectionsServer() {
        return serverConnections.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    private void sendPacket(IOFSwitch sw, FloodlightContext cntx, String serverIp) {
        // Lấy địa chỉ MAC của server
        MacAddress mac = getMacAddress(serverIp);
        // Ở đây, thêm logic để gửi lại gói tin qua OpenFlow nếu cần
    }

    private MacAddress getMacAddress(String ip) {
        // Giả định MAC của server tương ứng với IP
        if (ip.equals("10.0.0.1")) {
            return MacAddress.of("00:00:00:00:00:01");
        } else if (ip.equals("10.0.0.2")) {
            return MacAddress.of("00:00:00:00:00:02");
        }
        return MacAddress.NONE;
    }

    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleServices() {
        return null;
    }

    @Override
    public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
        return null;
    }

    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
        return Collections.singleton(IFloodlightProviderService.class);
    }

    @Override
    public void init(IFloodlightModuleContext context) throws FloodlightModuleException {
        this.floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
        this.serverConnections = new HashMap<>();
        this.servers = Arrays.asList("10.0.0.1", "10.0.0.2");
    }

    @Override
    public void startUp(FloodlightModuleContext context) {
        floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
    }
}
