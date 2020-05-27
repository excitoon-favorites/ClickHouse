#include <Access/Authentication.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <Poco/SHA1Engine.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

const String & Authentication::getLDAPServerName() const
{
    return ldap_server_name;
}

void Authentication::setLDAPServerName(const String & server_name)
{
    ldap_server_name = server_name;
}

Authentication::Digest Authentication::getPasswordDoubleSHA1() const
{
    switch (type)
    {
        case NO_PASSWORD:
        {
            Poco::SHA1Engine engine;
            return engine.digest();
        }

        case PLAINTEXT_PASSWORD:
        {
            Poco::SHA1Engine engine;
            engine.update(getPassword());
            const Digest & first_sha1 = engine.digest();
            engine.update(first_sha1.data(), first_sha1.size());
            return engine.digest();
        }

        case SHA256_PASSWORD:
            throw Exception("Cannot get password double SHA1 for user with 'SHA256_PASSWORD' authentication.", ErrorCodes::BAD_ARGUMENTS);

        case DOUBLE_SHA1_PASSWORD:
            return password_hash;

        case LDAP_PASSWORD:
            throw Exception("Cannot get password double SHA1 for user with 'LDAP_PASSWORD' authentication.", ErrorCodes::BAD_ARGUMENTS);
    }
    throw Exception("Unknown authentication type: " + std::to_string(static_cast<int>(type)), ErrorCodes::LOGICAL_ERROR);
}


bool Authentication::isCorrectPassword(const String & password_, const String & user_, const ExternalAuthenticators & external_authenticators) const
{
    switch (type)
    {
        case NO_PASSWORD:
            return true;

        case PLAINTEXT_PASSWORD:
        {
            if (password_ == std::string_view{reinterpret_cast<const char *>(password_hash.data()), password_hash.size()})
                return true;

            // For compatibility with MySQL clients which support only native authentication plugin, SHA1 can be passed instead of password.
            auto password_sha1 = encodeSHA1(password_hash);
            return password_ == std::string_view{reinterpret_cast<const char *>(password_sha1.data()), password_sha1.size()};
        }

        case SHA256_PASSWORD:
            return encodeSHA256(password_) == password_hash;

        case DOUBLE_SHA1_PASSWORD:
        {
            auto first_sha1 = encodeSHA1(password_);

            /// If it was MySQL compatibility server, then first_sha1 already contains double SHA1.
            if (first_sha1 == password_hash)
                return true;

            return encodeSHA1(first_sha1) == password_hash;
        }

        case LDAP_PASSWORD:
        {
            auto ldap_server_params = external_authenticators.getLDAPServerParams(ldap_server_name);
            ldap_server_params.user = user_;
            ldap_server_params.password = password_;
            LDAPSimpleAuthClient ldap_client(ldap_server_params);
            return ldap_client.check();
        }
    }
    throw Exception("Unknown authentication type: " + std::to_string(static_cast<int>(type)), ErrorCodes::LOGICAL_ERROR);
}

}
