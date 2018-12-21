#!/bin/env python
# -*- encoding: utf-8 -*-

USER_NO_AUTH='USER_NO_AUTH_TIMEOUT'
PSM_PREFIX = "psm_"
NO_TOKEN_STR = 'DO_NOT_HAS_TOKEN'


def get_token():
    try:
        from pyutil.infsec.sdk4dps.dps_token import DpsToken
        from pyutil.infsec.sdk4dps.exception import DpsException
        dpstoken = DpsToken()
        token = dpstoken.get_token()
        return token
    except DpsException, dps_err:
        return NO_TOKEN_STR
    except Exception, err:
        return NO_TOKEN_STR

def get_user_name_from_token():
    from pyutil.infsec.sdk4dps.dps_token import DpsToken
    from pyutil.infsec.sdk4dps.exception import DpsException
    user_name = USER_NO_AUTH
    try:
        dpstoken = DpsToken()
        token = dpstoken.get_token()
        identity = dpstoken.parse_token(token)
        token_dict = identity.__dict__
        if not token_dict.has_key('primaryAuthType'):
            return USER_NO_AUTH
        token_type = token_dict['primaryAuthType']
        if token_type is not None and token_type != "":
            if token_type.lower() == 'user' and token_dict['user'] != "":
                user_name = token_dict['user']
            elif token_type.lower() == 'psm' and token_dict['psm'] != "":
                user_name = PSM_PREFIX + token_dict['psm']
            else:
                user_name = USER_NO_AUTH
        return user_name
    except DpsException, dps_err:
        #print "WARN: DpsException faild to get username from token " + str(dps_err.message)
        return USER_NO_AUTH
    except Exception, err:
        #print "WARN: Exception faild to get username from token " + str(err.message)
        return USER_NO_AUTH