#include "system_config.h"
ASSERT_IS_POD(server_type);
ASSERT_IS_POD(server_info);
ASSERT_IS_POD(server_group);
ASSERT_IS_POD(system_config);

#include <tinyxml.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <optional>

inline namespace {

	/*
	check if an element has an attribute with a given name,
	allocating and copying the data to dest_ptr and returning true if it does,
	otherwise returning false and logging to stderr.
	*/
	bool copy_string_attribute(const TiXmlElement *node, const char *attr_name, char **dest_ptr) noexcept {
		const char *attr_ptr = node->Attribute(attr_name);

		if(attr_ptr == nullptr) {
			std::cerr << "Parser: bad member element: must have string attribute '" << attr_name << "'\n";

			return false;

		} else {
			*dest_ptr = strdup(attr_ptr);

			return true;
		}
	}

	/*
	check if an element has a non-negative integer attribute with a given name,
	copying the value to dest_ptr and returning true if it does, otherwise
	returning false and logging to stderr.
	*/
	bool get_unsigned_int_attribute(const TiXmlElement *node, const char *attr_name, uintmax_t *dest_ptr) noexcept {

		if(node->QueryValueAttribute(attr_name, dest_ptr) != TIXML_SUCCESS) {
			std::cerr << "Parser: bad member element: must have unsigned integer attribute '" << attr_name << "'\n";

			return false;

		} else return true;
	}


	/*
	check if an element has a positive floating-point attribute with a given name,
	copying the value to dest_ptr and returning true if it does, otherwise
	returning false and logging to stderr.
	*/
	bool get_positive_float_attribute(const TiXmlElement *node, const char *attr_name, float *dest_ptr) noexcept {

		if(node->QueryFloatAttribute(attr_name, dest_ptr) != TIXML_SUCCESS) {
			std::cerr << "Parser: bad member element: must have positive floating-point attribute '" << attr_name << "'\n";

		} else if(*dest_ptr == 0.0) {
			std::cerr << "Parser: bad attribute: '" << attr_name << "' must be a positive floating-point number (was 0)\n";

		} else return true;

		return false;
	}

	// verify that the name of an element is a given value
	bool elem_name_is(const TiXmlElement *elem, const char *name) noexcept {

		if(strcmp(name, elem->Value())) {

			std::cerr << "Parser: bad element type: expected '" << name << "', but got '" << elem->Value() << "'\n";

			return false;

		} else return true;
	}
	
	// helper to call update_server_from_string on a system_config until a socket_client runs out of updates to send
	std::vector<server_info*> process_resc_data(system_config *config, socket_client *client) {
		std::vector<server_info*> vec;
		client_send(client, "OK");
		std::string response = strcpy_and_free(client_receive(client));

		while(response != ".") {
			vec.push_back(config->update_server_from_string(response));
			client_send(client, "OK");
			response = strcpy_and_free(client_receive(client));
		}

		for(auto server : vec) {
			if(server->state == SS_INACTIVE || server->state == SS_UNAVAILABLE) continue;
			else if(server->state == SS_IDLE) {
				if(server->jobs != nullptr) {
					free(server->jobs);
					server->jobs = nullptr;
					server->num_jobs = 0;
				}
			} else server->update_jobs(client);
		}

		return vec;
	};
}

void server_type::release() noexcept {
	free(name);
}

void server_info::release() noexcept {
	if(jobs != nullptr) free(jobs);
}

void server_group::release() noexcept {
	free(const_cast<server_info**>(servers));
}

void system_config::release() noexcept {

	for(auto i = 0; i < num_servers; ++i) servers[i].release();

	free(servers);

	for(auto i = 0; i < num_types; ++i) const_cast<server_type*>(types)[i].release();

	free(const_cast<server_type*>(types));
}

void system_config::update(socket_client *client) {

	if(!client_msg_resp(client, "RESC All", "DATA")) throw std::runtime_error("Server did not respond as expected!");

	else process_resc_data(this, client);
}

void system_config::update(socket_client *client, const server_type *type) {
	std::ostringstream request;
	request << "RESC Type " << type->name;

	auto request_str = request.str(); // required for safety because this is otherwise a temporary object

	if(!client_msg_resp(client, request_str.c_str(), "DATA")) throw std::runtime_error("Server did not respond as expected!");

	else process_resc_data(this, client);
};

std::vector<server_info *> system_config::update(socket_client *client, const resource_info &resc) {
	std::ostringstream request;
	request << "RESC Avail " << resc.cores << " " << resc.memory << " " << resc.disk;

	auto request_str = request.str(); // required for safety because this is otherwise a temporary object

	if(!client_msg_resp(client, request_str.c_str(), "DATA")) throw std::runtime_error("Server did not respond as expected!");

	else return process_resc_data(this, client);
}

server_info *system_config::update_server_from_string(const std::string &str) {
	std::istringstream stream(str);
	std::string name;
	size_t id;
	int state;
	intmax_t time;
	resource_info resc;

	// use standard istream parsing to just split values at spaces, works great for our use-case
	stream >> name >> id >> state >> time >> resc.cores >> resc.memory >> resc.disk;

	auto *type = type_by_name(name.c_str());
	server_info *server = &start_of_type(type)[id];

	server->update(static_cast<server_state>(state), time, resc);

	return server;
};

void server_info::update_jobs(socket_client *client) {
	std::ostringstream request;
	request << "LSTJ " << type->name << " " << id;

	auto request_str = request.str(); // required for safety because this is otherwise a temporary object

	if(!client_msg_resp(client, request_str.c_str(), "DATA")) throw std::runtime_error("Server did not respond as expected!");

	std::vector<schd_info> vec;

	client_send(client, "OK");

	std::string response = strcpy_and_free(client_receive(client));

	while(response != ".") {
		std::istringstream stream(response);
		//size_t job_id;
		int state;
		schd_info schd;
		//intmax_t start_time;
		//uintmax_t est_runtime;
		//resource_info resc;

		stream >> schd.job_id >> state >> schd.start_time >> schd.est_runtime >> schd.req_resc.cores >> schd.req_resc.memory >> schd.req_resc.disk;

		if(state > 2) continue; // job has finished, effectively
		vec.push_back(schd);
		client_send(client, "OK");
		response = client_receive(client);
	}

	if(jobs != nullptr) free(jobs);
	if(vec.empty()) {
		jobs = nullptr;
		num_jobs = 0;
	}
	else {
		num_jobs = memcpy_from_vector(jobs, vec);
	}
}

system_config *parse_config(const char *path) noexcept {
	TiXmlDocument doc;
	if(!doc.LoadFile(path)) return nullptr;

	TiXmlElement *root = doc.RootElement();
	if(!elem_name_is(root, "system")) return nullptr;

	TiXmlElement *nodes = root->FirstChildElement();
	if(!elem_name_is(nodes, "servers")) return nullptr;

	auto types = std::vector<server_type>(); // use a vector to avoid over-alloc or realloc
	TiXmlElement *node; // declare outside of loop to check if it finished properly

	for(node = nodes->FirstChildElement(); node != nullptr && elem_name_is(node, "server"); node = node->NextSiblingElement()) {
		server_type type;

		if(!get_unsigned_int_attribute(node, "limit",&type.limit)) break;
		if(!get_unsigned_int_attribute(node, "bootupTime", &type.bootTime)) break;
		if(!get_positive_float_attribute(node, "rate", &type.rate)) break;
		if(!get_unsigned_int_attribute(node, "coreCount", &type.max_resc.cores)) break;
		if(!get_unsigned_int_attribute(node, "memory", &type.max_resc.memory)) break;
		if(!get_unsigned_int_attribute(node, "disk", &type.max_resc.disk)) break;
		if(!copy_string_attribute(node, "type", &type.name)) break;

		types.push_back(type);
	}

	if(node != nullptr) { // failure, free and return null
		for(auto type : types) free(type.name);

		return nullptr;
	} // otherwise success

	system_config *config = static_cast<system_config *>(malloc(sizeof(system_config)));

	// have to make a copy of the server_type-s first, otherwise the vector will
	//.. still own the memory they live in when we initialize server_info-s with them
	config->num_types = memcpy_from_vector(config->types, types);

	// use a vector for this, again to avoid over-alloc or realloc
	auto servers = std::vector<server_info>();

	for(auto t = 0; t < config->num_types; ++t) {
		auto *type = &config->types[t];

		for(size_t id = 0; id < type->limit; ++id) {
			servers.push_back(server_info{ type, id, server_state::SS_INACTIVE, 0, type->max_resc, nullptr, 0 });
		}
	}

	// we own these to begin with, so no problem here
	config->num_servers = memcpy_from_vector(config->servers, servers);

	return config;
}

void free_config(system_config *config) noexcept {
	config->release();
	free(config);
}

void free_group(server_group *group) noexcept {
	group->release();
	free(group);
}

const server_type *system_config::type_by_name(const char *name) const {

	for(auto t = 0; t < num_types; ++t) if(!strcmp(types[t].name, name)) return &types[t];

	throw std::invalid_argument("No type exists with requested name!");
}

const server_type *type_by_name(const system_config *config, const char* name) noexcept {
	try {
		return config->type_by_name(name);

	} catch(...) {

		return nullptr;
	}
}

server_info *system_config::start_of_type(const server_type *type) const {

	for(auto s = 0; s < num_servers; ++s) if(servers[s].type == type) return &servers[s];

	throw std::invalid_argument("No servers exist with requested type!");
};

server_info *start_of_type(const system_config *config, const server_type *type) noexcept {
	try {

		return config->start_of_type(type);

	} catch(...) {

		return nullptr;
	}
}

bool update_config(system_config *config, socket_client *client) noexcept {
	try {
		config->update(client);

		return true;

	} catch(...) {

		return false;
	}
};

bool update_servers_by_type(system_config *config, socket_client *client, const server_type *type) noexcept {
	try {
		config->update(client, type);

		return true;

	} catch(...) {

		return false;
	}
};

server_group *updated_servers_by_avail(system_config *config, socket_client *client, const resource_info resc) noexcept {
	try {
		auto vec = config->update(client, resc);
		server_group *group = static_cast<server_group*>(malloc(sizeof(server_group)));
		group->num_servers = memcpy_from_vector(group->servers, vec);

		return group;

	} catch(...) {

		return nullptr;
	}
};

bool server_info::update(server_state state, intmax_t time, const resource_info &resc) noexcept {

	if(resc <= type->max_resc) {

		this->state = state;
		this->avail_time = time;
		this->avail_resc = resc;

		return true;

	} else return false;
}

bool update_server(server_info *server, server_state state, intmax_t time, resource_info resc) noexcept {
	return server->update(state, time, resc);
}

void server_info::reset() noexcept {
	avail_resc = type->max_resc;
}

void reset_server(server_info *server) noexcept {
	//TODO: figure out what avail_time is, currently assuming it's "time until available"
	server->reset();
}