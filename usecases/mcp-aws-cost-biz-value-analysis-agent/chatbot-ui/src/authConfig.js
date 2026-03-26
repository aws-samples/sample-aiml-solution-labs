export const cognitoConfig = {
  userPoolId: "us-west-2_6AuG0cghr",
  userPoolClientId: "7sueti9imhqmlrubqvj97lqvse",
  region: "us-west-2",
  domain: "us-west-26aug0cghr",
  redirectSignIn: "https://d36jv11hejmhh5.cloudfront.net/",
  redirectSignOut: "https://d36jv11hejmhh5.cloudfront.net/",
  responseType: "code",
  scopes: ["email", "openid", "profile", "aws.cognito.signin.user.admin"],
};

export const getHostedUIUrl = () => {
  const { domain, userPoolClientId, redirectSignIn, scopes, responseType, region } = cognitoConfig;
  const scopeString = scopes.join(" ");
  return (
    `https://${domain}.auth.${region}.amazoncognito.com/oauth2/authorize` +
    `?identity_provider=FederateOIDC` +
    `&client_id=${userPoolClientId}` +
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
