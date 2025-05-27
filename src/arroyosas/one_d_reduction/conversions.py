import numpy as np


def filter_nans(data):
    """
    Remove numpy.NaNs from the intensity value of 1d reduced data

    Parameters
    ----------
    data : tuple(numpy.ndarray, numpy.ndarray, numpy.ndarray)
        Tuple with of arrays where the second one represents intensity

    Returns
    -------
    tuple(numpy.ndarray, numpy.ndarray, numpy.ndarray)

    """

    # Find masked values in the intensity array
    if np.ma.is_masked(data[1]):
        masked_indices = data[1].mask
    else:
        masked_indices = np.isnan(data[1])
    cleaned_data = tuple(array[~masked_indices] for array in data)
    return cleaned_data


def mask_image(image, mask):
    """
    Creates a masked array from an image and a mask, setting masked positions to NaN.

    Parameters
    ----------
    image : numpy.ndarray
        Input (detector) image
    mask : sequence or numpy.ndarray
        Mask. True indicates a masked (i.e. invalid) data.

    Returns
    -------
    numpy.ma.MaskedArray

    """
    if image.shape != mask.shape:
        mask = np.rot90(mask)

    masked_image = np.ma.masked_array(image, mask, dtype="float32", fill_value=np.nan)
    return masked_image


def degrees_to_radians(value):
    """
    Maps from an angle value in degrees [0,360] to [-pi, pi]
    """
    if value > 180:
        value -= 360
    return value * np.pi / 180


def angle_to_pix(a, sdd, pix_size):
    """Converts an angle in degree, to a length in pixel space.

    Parameters
    ----------
    a : float or numpy.ndarray
        angle in degree
    sdd : float
        sample-detector-distance in mm
    pix_size: float
        size of one pixel in µm

    Returns
    -------
    float or numpy.ndarray
    """
    return np.tan(a / 180 * np.pi) * sdd / (pix_size / 1000)


def pix_to_angle(pixels, sdd, pix_size):
    """Converts from length in pixel space to angle in degree.

    Parameters
    ----------
    pixels : float or numpy.ndarray
        pixel coordinate(s)
    sdd : float
        sample-detector-distance in mm
    pix_size: float
        size of one pixel in µm

    Returns
    -------
    float or numpy.ndarray
    """
    return np.arctan(pixels * (pix_size / 1000) / (sdd)) / np.pi * 180


def pix_to_alpha_f(pixels, sdd, pix_size, a_i):
    return pix_to_angle(pixels, sdd, pix_size) - a_i


def pix_to_theta_f(pixels, sdd, pix_size):
    return pix_to_angle(pixels, sdd, pix_size)


def q_z(wl, a_f, a_i):
    r"""Calculates the q_z component of the scattering vector.

    .. math::
    {q}_{z} = \frac{2 \pi}{\lambda} \sin(\alpha_f) + \sin(\alpha_i)
    """

    return 2 * np.pi / wl * (np.sin(a_f / 180 * np.pi) + np.sin(a_i / 180 * np.pi))


def q_y(wl, a_f, t_f):
    r"""Calculates the q_y component of the scattering vector.

    .. math::
    {q}_{y} = \frac{2 \pi}{\lambda} \sin(\theta_f) \cos(\alpha_f)
    """
    return 2 * np.pi / wl * np.sin(t_f / 180 * np.pi) * np.cos(a_f / 180 * np.pi)


def q_x(wl, t_f, a_f, a_i):
    r"""Calculates the q_x component of the scattering vector.

    .. math::
    {q}_{x} = \frac{2 \pi}{\lambda} (\cos(\alpha_f) * \cos(\theta_f) - \cos(\alpha_i))
    """
    return (
        2
        * np.pi
        / wl
        * (
            np.cos(t_f / 180 * np.pi) * np.cos(a_f / 180 * np.pi)
            - np.cos(a_i / 180 * np.pi)
        )
    )


def q_parallel(wl, t_f, a_f, a_i):
    qy = q_y(wl, a_f, t_f)
    qx = q_x(wl, t_f, a_f, a_i)
    return np.sign(qy) * np.sqrt(qx**2 + qy**2)


def qp_to_pix(q, wl, a_f, a_i, sdd, pix_size):
    t_f = (
        180
        / np.pi
        * np.arccos(
            (
                (
                    4 * np.pi**2
                    - wl**2 * q**2
                    + 2
                    * np.pi**2
                    * (np.cos(a_f * np.pi / 90) + np.cos(a_i * np.pi / 90))
                )
                * 1
                / (np.cos(a_f * np.pi / 180))
                * 1
                / (np.cos(a_i * np.pi / 180))
            )
            / (8 * np.pi**2)
        )
    )
    pix_y = angle_to_pix(t_f, sdd, pix_size)
    return pix_y
