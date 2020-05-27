#include <Access/LDAPClient.h>
#include <Common/Exception.h>
#include <ext/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
    extern const int LDAP_ERROR;
}

LDAPClient::LDAPClient(const LDAPServerParams & params_)
    : params(params_)
{
}

LDAPClient::~LDAPClient()
{
    closeConnection();
}

#if USE_LDAP

namespace
{
    auto escapeForLDAP(const String & src)
    {
        String dest;
        dest.reserve(src.size() * 2);

        for (auto ch : src)
        {
            switch (ch)
            {
                case ',':
                case '\\':
                case '#':
                case '+':
                case '<':
                case '>':
                case ';':
                case '"':
                case '=':
                    dest += '\\';
                    break;
            }
            dest += ch;
        }

        return dest;
    }
}

void LDAPClient::diag(const int rc)
{
    if (rc != LDAP_SUCCESS)
    {
        String text;
        const char * raw_err_str = ldap_err2string(rc);

        if (raw_err_str)
            text = raw_err_str;

        if (handle)
        {
            String message;
            char * raw_message = nullptr;
            ldap_get_option(handle, LDAP_OPT_DIAGNOSTIC_MESSAGE, &raw_message);

            if (raw_message)
            {
                message = raw_message;
                ldap_memfree(raw_message);
                raw_message = nullptr;
            }

            if (!message.empty())
            {
                if (!text.empty())
                    text += ": ";
                text += message;
            }
        }

        throw Exception(text, ErrorCodes::LDAP_ERROR);
    }
}

int LDAPClient::openConnection(const bool graceful_bind_failure)
{
    closeConnection();

    {
        LDAPURLDesc url;
        std::memset(&url, 0, sizeof(url));

        url.lud_scheme = const_cast<char *>(params.enable_tls == LDAPServerParams::TLSEnable::YES ? "ldaps" : "ldap");
        url.lud_host = const_cast<char *>(params.host.c_str());
        url.lud_port = params.port;
        url.lud_scope = LDAP_SCOPE_DEFAULT;

        auto * uri = ldap_url_desc2str(&url);
        if (!uri)
            throw Exception("ldap_url_desc2str() failed", ErrorCodes::LDAP_ERROR);

        SCOPE_EXIT({ ldap_memfree(uri); });

        diag(ldap_initialize(&handle, uri));
        if (!handle)
            throw Exception("ldap_initialize() failed", ErrorCodes::LDAP_ERROR);
    }

    {
        int value = 0;
        switch (params.protocol_version)
        {
            case LDAPServerParams::ProtocolVersion::V2: value = LDAP_VERSION2; break;
            case LDAPServerParams::ProtocolVersion::V3: value = LDAP_VERSION3; break;
        }
        diag(ldap_set_option(handle, LDAP_OPT_PROTOCOL_VERSION, &value));
    }

    diag(ldap_set_option(handle, LDAP_OPT_RESTART, LDAP_OPT_ON));
    diag(ldap_set_option(handle, LDAP_OPT_KEEPCONN, LDAP_OPT_ON));

    {
        ::timeval operation_timeout;
        operation_timeout.tv_sec = params.operation_timeout.count();
        operation_timeout.tv_usec = 0;
        diag(ldap_set_option(handle, LDAP_OPT_TIMEOUT, &operation_timeout));
    }

    {
        ::timeval network_timeout;
        network_timeout.tv_sec = params.network_timeout.count();
        network_timeout.tv_usec = 0;
        diag(ldap_set_option(handle, LDAP_OPT_NETWORK_TIMEOUT, &network_timeout));
    }

    {
        const int search_timeout = params.search_timeout.count();
        diag(ldap_set_option(handle, LDAP_OPT_TIMELIMIT, &search_timeout));
    }

    {
        const int size_limit = params.search_limit;
        diag(ldap_set_option(handle, LDAP_OPT_SIZELIMIT, &size_limit));
    }

    {
        int value = 0;
        switch (params.tls_cert_verify)
        {
            case LDAPServerParams::TLSCertVerify::NEVER:  value = LDAP_OPT_X_TLS_NEVER;  break;
            case LDAPServerParams::TLSCertVerify::ALLOW:  value = LDAP_OPT_X_TLS_ALLOW;  break;
            case LDAPServerParams::TLSCertVerify::TRY:    value = LDAP_OPT_X_TLS_TRY;    break;
            case LDAPServerParams::TLSCertVerify::DEMAND: value = LDAP_OPT_X_TLS_DEMAND; break;
        }
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_REQUIRE_CERT, &value));
    }

    if (!params.ca_cert_dir.empty())
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_CACERTDIR, params.ca_cert_dir.c_str()));

    if (!params.ca_cert_file.empty())
        diag(ldap_set_option(handle, LDAP_OPT_X_TLS_CACERTFILE, params.ca_cert_file.c_str()));

    if (params.enable_tls == LDAPServerParams::TLSEnable::YES_STARTTLS)
        diag(ldap_start_tls_s(handle, nullptr, nullptr));

    int rc = LDAP_OTHER;

    switch (params.sasl_mechanism)
    {
        case LDAPServerParams::SASLMechanism::SIMPLE:
        {
            const String dn = params.auth_dn_prefix + escapeForLDAP(params.user) + params.auth_dn_suffix;

            ::berval cred;
            cred.bv_val = const_cast<char *>(params.password.c_str());
            cred.bv_len = params.password.size();

            rc = ldap_sasl_bind_s(handle, dn.c_str(), LDAP_SASL_SIMPLE, &cred, nullptr, nullptr, nullptr);

            if (!graceful_bind_failure)
                diag(rc);

            break;
        }
    }

    return rc;
}

void LDAPClient::openConnection()
{
    const bool graceful_bind_failure = false;
    diag(openConnection(graceful_bind_failure));
}

void LDAPClient::closeConnection() noexcept
{
    if (!handle)
        return;

    ldap_unbind_ext_s(handle, nullptr, nullptr);
    handle = nullptr;
}

bool LDAPSimpleAuthClient::check()
{
    bool result = false;

    if (params.user.empty())
        throw Exception("LDAP authentication of a user with an empty name is not allowed", ErrorCodes::BAD_ARGUMENTS);

    const bool graceful_bind_failure = true;
    const auto rc = openConnection(graceful_bind_failure);

    SCOPE_EXIT({ closeConnection(); });

    switch (rc)
    {
        case LDAP_SUCCESS:
        {
            result = true;
            break;
        }

        case LDAP_INVALID_CREDENTIALS:
        {
            result = false;
            break;
        }

        default:
        {
            result = false;
            diag(rc);
            break;
        }
    }

    return result;
}

#else // USE_LDAP

void LDAPClient::diag(const int)
{
    throw Exception("ClickHouse was built without LDAP support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
}

int LDAPClient::openConnection(const bool)
{
    throw Exception("ClickHouse was built without LDAP support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
}

void LDAPClient::openConnection()
{
    throw Exception("ClickHouse was built without LDAP support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
}

void LDAPClient::closeConnection() noexcept
{
}

bool LDAPSimpleAuthClient::check()
{
    throw Exception("ClickHouse was built without LDAP support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
}

#endif // USE_LDAP

}
