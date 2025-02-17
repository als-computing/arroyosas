import torch.nn as nn


class CNNAutoencoder(nn.Module):
    def __init__(self, params=None, **kwargs):
        super(CNNAutoencoder, self).__init__()
        if params is None:
            self.latent_dim = 1000
        else:
            self.latent_dim = params.latent_dim

        # Encoder
        self.encoder = nn.Sequential(
            nn.Conv2d(1, 32, 3, stride=2, padding=1),
            nn.ReLU(),
            nn.Conv2d(32, 64, 3, stride=2, padding=1),
            nn.ReLU(),
            nn.Conv2d(64, 128, 3, stride=2, padding=1),
            nn.ReLU(),
            nn.Conv2d(128, 256, 3, stride=2, padding=1),
            nn.ReLU(),
            nn.Conv2d(256, 512, 7),  # Last convolution
            nn.Flatten(),  # Flatten to vector for Linear Layer
            nn.Linear(
                2 * 2 * 512, self.latent_dim
            ),  # Linear layer to map to latent space
        )

        # Decoder
        self.decoder = nn.Sequential(
            nn.ConvTranspose2d(512, 256, 7),
            nn.ReLU(),
            nn.ConvTranspose2d(256, 128, 3, stride=2, padding=1, output_padding=1),
            nn.ReLU(),
            nn.ConvTranspose2d(128, 64, 3, stride=2, padding=1, output_padding=1),
            nn.ReLU(),
            nn.ConvTranspose2d(64, 32, 3, stride=2, padding=1, output_padding=1),
            nn.ReLU(),
            nn.ConvTranspose2d(32, 1, 3, stride=2, padding=1, output_padding=1),
            nn.Tanh(),
        )

        self.linear_layer = nn.Linear(self.latent_dim, 2 * 2 * 512)

    def forward(self, x):
        latent = self.encoder(x)
        latent_matrix = self.linear_layer(latent)
        reshaped_latent = latent_matrix.reshape(x.shape[0], 512, 2, 2)
        decoded = self.decoder(reshaped_latent)
        return decoded

    def embed_imgs(self, x):
        return self.encoder(x)  # Get the latent representation (no decoding)


# Function to create the Autoencoder model using params
def simple_autoencoder(params=None, **kwargs):
    model = CNNAutoencoder(params, **kwargs)
    return model
