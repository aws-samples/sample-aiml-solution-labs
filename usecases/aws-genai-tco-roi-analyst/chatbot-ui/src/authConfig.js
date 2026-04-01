// All account-specific values are read from environment variables.
// Set these in your .env file before building. See .env.example for details.
export const cognitoConfig = {
  userPoolId: process.env.REACT_APP_COGNITO_USER_POOL_ID || "REPLACE_ME",
  userPoolClientId: process.env.REACT_APP_COGNITO_CLIENT_ID || "REPLACE_ME",
  region: process.env.REACT_APP_AWS_REGION || "us-west-2",
  domain: process.env.REACT_APP_COGNITO_DOMAIN || "REPLACE_ME",
  redirectSignIn: process.env.REACT_APP_REDIRECT_URL || "https://your-cloudfront-domain.cloudfront.net/",
  redirectSignOut: process.env.REACT_APP_REDIRECT_URL || "https://your-cloudfront-domain.cloudfront.net/",
  responseType: "code",
  scopes: ["email", "openid", "profile", "aws.cognito.signin.user.admin"],
};

export const getHostedUIUrl = () => {
  const { domain, userPoolClientId, redirectSignIn, scopes, responseType, region } = cognitoConfig;
  const scopeString = scopes.join(" ");
  return (
    `https://${domain}.auth.${region}.amazoncognito.com/oauth2/authorize` +
    `?client_id=${userPoolClientId}` +
    `&response_type=${responseType}` +
    `&scope=${encodeURIComponent(scopeString)}` +
    `&redirect_uri=${encodeURIComponent(redirectSignIn)}`
  );
};

export const getLogoutUrl = () => {
  const { domain, userPoolClientId, redirectSignOut, region } = cognitoConfig;
  return (
    `https://${domain}.auth.${region}.amazoncognito.com/logout` +
    `?client_id=${userPoolClientId}` +
    `&logout_uri=${encodeURIComponent(redirectSignOut)}`
  );
};
