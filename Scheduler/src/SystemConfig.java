import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.*;

public final class SystemConfig {
    public final class ServerType {
        final String name;
        final int    limit;
        final int    bootTime; // seconds
        final float  rate;
        final int    cores;
        final int    memory; // megabytes
        final int    disk; // megabytes
        
        /**
         * Instantiates and validates a ServerType, describing a set of instanced resources
         *
         * @param name     the name of the server type
         * @param limit    how many concurrent instances can exist
         * @param bootTime how long it takes to start an instance, in seconds
         * @param rate     //TODO: figure out what this means
         * @param cores    how many virtual CPU cores the server type offers
         * @param memory   how much memory the server type offers, in megabytes
         * @param disk     how much disk space the server type offers, in megabytes
         * @throws IllegalArgumentException when any of the arguments are negative, irrational,
         *                                  or resource arguments are zero
         */
        private ServerType(String name, int limit, int bootTime, float rate, int cores, int memory, int disk)
        throws IllegalArgumentException {
            if(name == null) throw new NullPointerException("`name` cannot be null");
            if(limit < 0) throw new IllegalArgumentException("`limit` cannot be negative");
            if(bootTime < 0) throw new IllegalArgumentException("`bootTime` cannot be negative");
            if(rate < 0.0 || Float.isNaN(rate) || Float.isInfinite(rate)) {
                throw new IllegalArgumentException("`rate` must be a non-negative rational number");
            }
            if(cores <= 0) throw new IllegalArgumentException("`cores` must be positive");
            if(memory <= 0) throw new IllegalArgumentException("`memory` must be positive");
            if(disk <= 0) throw new IllegalArgumentException("`disk` must be positive");
            this.name = name;
            this.limit = limit;
            this.bootTime = bootTime;
            this.rate = rate;
            this.cores = cores;
            this.memory = memory;
            this.disk = disk;
        }
        
        /**
         * Instantiates a ServerType from an XML Element, used internally for parsing
         *
         * @param element an XML element
         * @throws IllegalArgumentException when the element does not represent a valid server type
         */
        private ServerType(Element element)
        throws IllegalArgumentException {
            this(element.getAttribute("type"),
                 Integer.parseInt(element.getAttribute("limit")),
                 Integer.parseInt(element.getAttribute("bootupTime")),
                 Float.parseFloat(element.getAttribute("rate")),
                 Integer.parseInt(element.getAttribute("coreCount")),
                 Integer.parseInt(element.getAttribute("memory")),
                 Integer.parseInt(element.getAttribute("disk"))
            );
        }
        
        @Override
        public boolean equals(Object obj) {
            if(this == obj) {
                return true;
            } else if(obj instanceof ServerType) {
                ServerType other = (ServerType) obj;
                return this.name.equals(other.name)
                       && this.limit == other.limit
                       && this.bootTime == other.bootTime
                       && this.rate == other.rate
                       && this.cores == other.cores
                       && this.memory == other.memory
                       && this.disk == other.disk;
            } else {
                return false;
            }
        }
        
        @Override
        public int hashCode() {
            return name.hashCode() ^ limit ^ bootTime ^ Float.hashCode(rate) ^ cores ^ memory ^ disk;
        }
    }
    
    
    enum ServerState {
        Offline, Starting, Idle, Active, Unavailable;
        
        public static ServerState valueOf(int state) {
            switch(state) {
                case 0:
                    return Offline;
                case 1:
                    return Starting;
                case 2:
                    return Idle;
                case 3:
                    return Active;
                case 4:
                    return Unavailable;
                default:
                    throw new IllegalArgumentException(String.format("%d does not map to a ServerState value", state));
            }
        }
    }
    
    
    public final class ServerInfo {
        final ServerType type;
        final int        id;
        private ServerState state = ServerState.Offline;
        //TODO: figure out what this is
        private int availableTime;
        private int availableCores;
        private int availableMemory; // megabytes
        private int availableDisk; // megabytes
        
        /**
         * Instantiates a ServerInfo, setting available resources to maximum
         *
         * @param type the type of server
         * @param id   the identifier of the server
         */
        private ServerInfo(ServerType type, int id) {
            this.type = type;
            this.id = id;
            this.availableCores = type.cores;
            this.availableMemory = type.memory;
            this.availableDisk = type.disk;
        }
        
        /**
         * Releases the specified resource quantities
         *
         * @param cores  to release
         * @param memory to release, in megabytes
         * @param disk   to release, in megabytes
         * @return the modified ServerInfo for method chaining
         * @throws IllegalArgumentException when any arguments are negative or beyond the maximum server capacity
         */
        public ServerInfo releaseResources(int cores, int memory, int disk)
        throws IllegalArgumentException {
            if(cores < 0 || memory < 0 || disk < 0) {
                throw new IllegalArgumentException("All arguments must be non-negative");
            } else if(cores > type.cores || memory > type.memory || disk > type.disk) {
                throw new IllegalArgumentException("Arguments must be within capacity of server type");
            }
            this.availableCores = Math.min(availableCores + cores, type.cores);
            this.availableMemory = Math.min(availableMemory + memory, type.memory);
            this.availableDisk = Math.min(availableDisk + disk, type.disk);
            return this;
        }
        
        /**
         * Releases all currently used resources
         *
         * @return the modified ServerInfo for method chaining
         */
        public ServerInfo releaseResources() {
            //TODO: determine what availableTime should be set to
            this.availableCores = type.cores;
            this.availableMemory = type.memory;
            this.availableDisk = type.disk;
            return this;
        }
        
        /**
         * Attempt to release resources
         *
         * @return whether the resources were released
         */
        public boolean tryReleaseResources(int cores, int memory, int disk) {
            try {
                this.releaseResources(cores, memory, disk);
                return true;
            } catch(IllegalArgumentException e) {
                return false;
            }
        }
        
        /**
         * Updates a ServerInfo in-place with a new internal state
         *
         * @param state  the new state
         * @param time   the new available time, in seconds
         * @param cores  the new available cores
         * @param memory the new free memory capacity, in megabytes
         * @param disk   the new available disk space, in megabytes
         * @return the updated ServerInfo, for method chaining
         * @throws IllegalArgumentException when the arguments are negative or outside the capacity of the server type
         */
        public ServerInfo update(ServerState state, int time, int cores, int memory, int disk)
        throws IllegalArgumentException {
            if(time < 0 || cores < 0 || memory < 0 || disk < 0) {
                throw new IllegalArgumentException("All arguments must be non-negative");
            } else if(cores > type.cores || memory > type.memory || disk > type.disk) {
                throw new IllegalArgumentException("Arguments must be within capacity of server type");
            } else {
                this.state = state;
                availableTime = time;
                availableCores = cores;
                availableMemory = memory;
                availableDisk = disk;
            }
            return this;
        }
        
        /**
         * Attempt to update a ServerInfo in-place
         *
         * @param state  the new state
         * @param time   the new available time, in seconds
         * @param cores  the new available cores
         * @param memory the new free memory capacity, in megabytes
         * @param disk   the new available disk space, in megabytes
         * @return whether the update succeeded or not
         */
        public boolean tryUpdate(ServerState state, int time, int cores, int memory, int disk) {
            try {
                update(state, time, cores, memory, disk);
                return true;
            } catch(IllegalArgumentException e) {
                return false;
            }
        }
        
        public ServerState getState() {
            return state;
        }
        
        /**
         * @return the available server time, in seconds
         */
        public int getAvailableTime() {
            return availableTime;
        }
        
        /**
         * @return the available virtual CPU cores
         */
        public int getAvailableCores() {
            return availableCores;
        }
        
        /**
         * @return the free memory capacity, in megabytes
         */
        public int getAvailableMemory() {
            return availableMemory;
        }
        
        /**
         * @return the available disk space, in megabytes
         */
        public int getAvailableDisk() {
            return availableDisk;
        }
        
        /**
         * Check if two servers are the same. Only considers type and id.
         *
         * @param obj the other server
         * @return whether `obj` is a ServerInfo with matching type and id
         */
        @Override
        public boolean equals(Object obj) {
            if(this == obj) {
                return true;
            } else if(obj instanceof ServerInfo) {
                ServerInfo other = (ServerInfo) obj;
                return this.type == other.type && this.id == other.id;
                // only compare on these two attributes as the others can change and it'll still be the same server
            } else {
                return false;
            }
        }
        
        @Override
        public int hashCode() {
            return this.type.hashCode() ^ id;
        }
    }
    
    
    private final Map<String, ServerType>           serverTypes;
    private final Map<ServerType, List<ServerInfo>> servers;
    
    /**
     * Instantiates a SystemConfig from an XML Document, performing validation
     *
     * @param doc the XML Document
     * @throws IllegalArgumentException when the document does not represent a valid system configuration
     */
    private SystemConfig(Document doc)
    throws IllegalArgumentException {
        if(doc == null) throw new NullPointerException("`doc` cannot be null");
        Element root = doc.getDocumentElement();
        if(!root.getNodeName().equals("system")) {
            throw new IllegalArgumentException(String.format("Unexpected element '%s' instead of 'system'",
                                                             root.getNodeName()
            ));
        }
        NodeList nodes = root.getChildNodes();
        Map<String, ServerType> serverTypes = new HashMap<>();
        for(int i = 0; i < nodes.getLength(); ++i) {
            Node node = nodes.item(i);
            switch(node.getNodeType()) {
                case Node.ELEMENT_NODE:
                    String nodeName = node.getNodeName();
                    if(nodeName.equals("servers")) {
                        NodeList servers = node.getChildNodes();
                        for(int j = 0; j < servers.getLength(); ++j) {
                            Node server = servers.item(j);
                            switch(server.getNodeType()) {
                                case Node.ELEMENT_NODE:
                                    if(server.getNodeName().equals("server")) {
                                        ServerType type = new ServerType((Element) server);
                                        serverTypes.put(type.name, type);
                                    } else {
                                        throw new IllegalArgumentException(String.format(
                                                "Unexpected element '%s' under 'servers'",
                                                server.getNodeName()
                                        ));
                                    }
                                case Node.TEXT_NODE:
                                case Node.COMMENT_NODE:
                                    // allow comments and text
                                    continue;
                                default:
                                    throw new IllegalArgumentException("Unexpected node type under 'servers'");
                            }
                        }
                    } else {
                        throw new IllegalArgumentException(String.format("Unexpected element '%s' under 'system'",
                                                                         nodeName
                        ));
                    }
                case Node.TEXT_NODE:
                case Node.COMMENT_NODE:
                    // allow comments and text
                    continue;
                default:
                    throw new IllegalArgumentException("Unexpected node type under 'system'");
            }
        }
        this.serverTypes = Collections.unmodifiableMap(serverTypes);
        Map<ServerType, List<ServerInfo>> servers = new HashMap<>();
        for(ServerType type : serverTypes.values()) {
            List<ServerInfo> group = new ArrayList<>(type.limit);
            for(int i = 0; i < type.limit; ++i) group.add(new ServerInfo(type, i));
            // make sure the id of a server matches the index in its list
            servers.put(type, Collections.unmodifiableList(group));
        }
        this.servers = Collections.unmodifiableMap(servers);
    }
    
    /**
     * Instantiates a SystemConfig from a stream containing an XML Document, performing validation
     *
     * @param stream the stream containing the XML document
     * @return a valid system configuration
     * @throws IOException              when there was a problem reading from the stream
     * @throws IllegalArgumentException when the stream does not represent a valid system configuration
     */
    public static SystemConfig fromStream(InputStream stream)
    throws IOException, IllegalArgumentException {
        try {
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(stream);
            return new SystemConfig(doc);
        } catch(ParserConfigurationException e) {
            // should never happen because we use the default configuration
            throw new RuntimeException("Default parser configuration failed!", e);
        } catch(SAXException e) {
            throw new IllegalArgumentException("Invalid XML format", e);
        }
    }
    
    /**
     * Instantiates a SystemConfig from a file containing an XML Document, performing validation
     *
     * @param file the file containing an XML Document
     * @return a valid system configuration
     * @throws IOException              when there was a problem reading the file
     * @throws IllegalArgumentException when the file does not represent a valid system configuration
     */
    public static SystemConfig fromFile(File file)
    throws IOException, IllegalArgumentException {
        InputStream stream = new FileInputStream(file);
        return SystemConfig.fromStream(stream);
    }
    
    /**
     * Instantiates a SystemConfig from a string containing an XML Document, performing validation
     *
     * @param str the string containing the XML Document
     * @return a valid system configuration
     * @throws IllegalArgumentException when the string does not represent a valid system configuration
     */
    public static SystemConfig fromString(String str)
    throws IllegalArgumentException {
        try {
            InputStream stream = new ByteArrayInputStream(str.getBytes());
            return SystemConfig.fromStream(stream);
        } catch(IOException e) {
            // should never happen because we aren't really doing IO
            throw new RuntimeException("Could not process IO on String!", e);
        }
    }
    
    /**
     * Get an unmodifiable collection of server types
     *
     * @return the server types
     */
    public Collection<ServerType> getServerTypes() {
        return Collections.unmodifiableCollection(serverTypes.values());
    }
    
    /**
     * Get a server type matching a name
     *
     * @param name the name of the type
     * @return the server type
     */
    public ServerType getServerType(String name) {
        if(name == null) throw new NullPointerException("`name` cannot be null");
        ServerType type = serverTypes.get(name);
        if(type == null) throw new NoSuchElementException(String.format("'%s' is not a known server type", name));
        return serverTypes.get(name);
    }
    
    /**
     * Get an unmodifiable collection of all servers
     *
     * @return the servers
     */
    public Collection<ServerInfo> getServers() {
        Collection<ServerInfo> temp = new ArrayList<>();
        for(List<ServerInfo> group : servers.values()) temp.addAll(group);
        return Collections.unmodifiableCollection(temp);
    }
    
    /**
     * Get an unmodifiable list of servers with a specific type
     *
     * @param type the server type
     * @return the servers with that type
     */
    public List<ServerInfo> getServers(ServerType type) {
        if(type == null) throw new NullPointerException("`type` must not be null");
        List<ServerInfo> group = servers.get(type);
        if(group == null) return Collections.unmodifiableList(new ArrayList<>());
        return group;
    }
    
    /**
     * Get a specific server by type and id
     *
     * @param type the server type
     * @param id   the server's id
     * @return the server
     */
    public ServerInfo getServer(ServerType type, int id) {
        return this.getServers(type).get(id);
    }
}